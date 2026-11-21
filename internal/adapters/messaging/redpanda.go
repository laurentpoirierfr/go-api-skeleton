package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// RedpandaConfig règle la connexion à Redpanda (ou à Kafka : le protocole est
// le même).
type RedpandaConfig struct {
	Brokers           []string
	Partitions        int32 // par topic : le parallélisme maximal d'un consommateur
	ReplicationFactor int16 // 1 en développement, 3 en production
}

// Redpanda est le broker Redpanda. Différences avec JetStream :
//
//   - pas de déduplication à la publication : un message republié par le
//     forwarder arrive deux fois, et c'est l'idempotence du consommateur
//     (processed_messages) qui l'absorbe ;
//   - pas de relivraison différée : le consommateur l'émule, en bloquant la
//     partition pendant le délai (emulateRedelivery) ;
//   - l'ordre est garanti par partition : la clé est l'identifiant de la
//     commande, les événements d'une même commande restent donc dans l'ordre.
type Redpanda struct {
	cfg    RedpandaConfig
	logger watermill.LoggerAdapter
	pub    *lazyPublisher

	mu    sync.Mutex
	admin sarama.ClusterAdmin
}

// NewRedpanda prépare le broker, sans se connecter : la connexion est ouverte
// au premier usage, pour qu'un binaire démarre même si Redpanda ne répond pas
// encore.
func NewRedpanda(cfg RedpandaConfig, logger watermill.LoggerAdapter) (*Redpanda, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("no Redpanda broker")
	}
	r := &Redpanda{cfg: cfg, logger: logger}
	r.pub = &lazyPublisher{create: func() (message.Publisher, error) {
		return kafka.NewPublisher(kafka.PublisherConfig{
			Brokers:               cfg.Brokers,
			Marshaler:             partitionByOrder,
			OverwriteSaramaConfig: producerConfig(),
		}, logger)
	}}
	return r, nil
}

// partitionByOrder place les événements d'une même commande dans la même
// partition (clé = ce-subject), donc dans l'ordre.
var partitionByOrder = kafka.NewWithPartitioningMarshaler(func(_ string, msg *message.Message) (string, error) {
	return msg.Metadata.Get(HeaderSubject), nil
})

// producerConfig rend le producteur idempotent : ses propres réessais ne
// créent pas de doublon.
func producerConfig() *sarama.Config {
	c := kafka.DefaultSaramaSyncPublisherConfig()
	c.Version = sarama.V2_8_0_0
	c.Producer.Idempotent = true
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Net.MaxOpenRequests = 1
	return c
}

func (r *Redpanda) Name() string                 { return "redpanda" }
func (r *Redpanda) Publisher() message.Publisher { return r.pub }
func (r *Redpanda) DelaysRedelivery() bool       { return false }

// SubscribeTopics : un consommateur Kafka s'abonne topic par topic.
func (r *Redpanda) SubscribeTopics() []string {
	return []string{TopicOrderCreated, TopicOrderCancelled}
}

// Subscriber crée un consommateur du groupe c.Name. Un nouveau groupe lit
// depuis le début : les événements publiés avant le premier démarrage du
// worker ne sont pas perdus.
func (r *Redpanda) Subscriber(c ConsumerConfig) (message.Subscriber, error) {
	sc := kafka.DefaultSaramaSubscriberConfig()
	sc.Version = sarama.V2_8_0_0
	sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	return kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:               r.cfg.Brokers,
		Unmarshaler:           partitionByOrder,
		ConsumerGroup:         c.Name,
		OverwriteSaramaConfig: sc,
		// Le délai entre deux livraisons est géré par emulateRedelivery.
		NackResendSleep:     kafka.NoSleep,
		ReconnectRetrySleep: time.Second,
	}, r.logger)
}

// EnsureTopology crée les topics des événements et ceux des DLQ.
func (r *Redpanda) EnsureTopology(ctx context.Context, consumers ...ConsumerConfig) error {
	topics := r.SubscribeTopics()
	for _, c := range consumers {
		topics = append(topics, DLQTopic(c.Name))
	}
	return r.withAdmin(ctx, func(admin sarama.ClusterAdmin) error {
		existing, err := admin.ListTopics()
		if err != nil {
			return err
		}
		for _, t := range topics {
			if _, ok := existing[t]; ok {
				continue
			}
			err := admin.CreateTopic(t, &sarama.TopicDetail{NumPartitions: r.cfg.Partitions, ReplicationFactor: r.cfg.ReplicationFactor}, false)
			if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
				return fmt.Errorf("topic %s: %w", t, err)
			}
		}
		return nil
	})
}

// Check vérifie qu'un broker du cluster répond.
func (r *Redpanda) Check(ctx context.Context) error {
	return r.withAdmin(ctx, func(admin sarama.ClusterAdmin) error {
		_, _, err := admin.DescribeCluster()
		return err
	})
}

func (r *Redpanda) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	if r.admin != nil {
		errs = append(errs, r.admin.Close())
	}
	errs = append(errs, r.pub.Close())
	return errors.Join(errs...)
}

// withAdmin ouvre le client d'administration au premier usage, et l'appel
// respecte ctx (Sarama ne prend pas de contexte). En cas d'échec, le client
// est refermé : le prochain appel se reconnecte.
func (r *Redpanda) withAdmin(ctx context.Context, f func(sarama.ClusterAdmin) error) error {
	r.mu.Lock()
	if r.admin == nil {
		c := sarama.NewConfig()
		c.Version = sarama.V2_8_0_0
		c.Net.DialTimeout = 3 * time.Second
		c.Admin.Timeout = 5 * time.Second
		admin, err := sarama.NewClusterAdmin(r.cfg.Brokers, c)
		if err != nil {
			r.mu.Unlock()
			return fmt.Errorf("redpanda: %w", err)
		}
		r.admin = admin
	}
	admin := r.admin
	r.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- f(admin) }()
	select {
	case err := <-done:
		if err != nil {
			r.mu.Lock()
			if r.admin == admin {
				_ = admin.Close()
				r.admin = nil
			}
			r.mu.Unlock()
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// lazyPublisher crée le publieur au premier message : Sarama se connecte dès
// sa création, et l'API doit démarrer même si le broker est indisponible (les
// événements attendent dans l'outbox). Un échec de création est réessayé au
// message suivant.
type lazyPublisher struct {
	mu     sync.Mutex
	create func() (message.Publisher, error)
	pub    message.Publisher
	closed bool
}

func (l *lazyPublisher) Publish(topic string, msgs ...*message.Message) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errors.New("publisher closed")
	}
	if l.pub == nil {
		pub, err := l.create()
		if err != nil {
			l.mu.Unlock()
			return fmt.Errorf("redpanda publisher: %w", err)
		}
		l.pub = pub
	}
	pub := l.pub
	l.mu.Unlock()
	return pub.Publish(topic, msgs...)
}

func (l *lazyPublisher) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.pub != nil {
		return l.pub.Close()
	}
	return nil
}
