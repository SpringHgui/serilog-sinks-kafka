﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog.Context;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.Kafka
{
    public class KafkaSink : ILogEventSink
    {
        private readonly TopicPartition _globalTopicPartition;
        private readonly ITextFormatter _formatter;
        private readonly Func<LogEvent, string> _topicDecider;
        private IProducer<Null, byte[]> _producer;
        Action<IProducer<Null, byte[]>, Error> _errorHandler;
        ProducerConfig _producerConfig;

        const string SKIP_KEY = "skip-kafka";

        public KafkaSink(
            ProducerConfig producerConfig,
            string topic = null,
            Func<LogEvent, string> topicDecider = null,
             ITextFormatter formatter = null, Action<IProducer<Null, byte[]>, Error> errorHandler = null)
        {
            _formatter = formatter ?? new Formatting.Json.JsonFormatter(renderMessage: true);

            if (topic != null)
                _globalTopicPartition = new TopicPartition(topic, Partition.Any);

            if (topicDecider != null)
                _topicDecider = topicDecider;

            if (_errorHandler != null)
                _errorHandler = errorHandler;
            else
            {
                _errorHandler = (pro, msg) =>
                {
                    Log.ForContext(SKIP_KEY, string.Empty).Error($"[KafkaError] {pro.Name} {msg.Code} {msg.Reason}");
                };
            }

            this._producerConfig = producerConfig;
            ConfigureKafkaConnection();
        }

        public void Emit(LogEvent logEvent)
        {
            if (logEvent.Properties.ContainsKey(SKIP_KEY))
                return;

            Message<Null, byte[]> message;

            var topicPartition = _topicDecider == null
                ? _globalTopicPartition
                : new TopicPartition(_topicDecider(logEvent), Partition.Any);

            using (var render = new StringWriter(CultureInfo.InvariantCulture))
            {
                _formatter.Format(logEvent, render);

                message = new Message<Null, byte[]>
                {
                    Value = Encoding.UTF8.GetBytes(render.ToString())
                };
            }

            _producer.Produce(topicPartition, message);
        }

        private void ConfigureKafkaConnection()
        {
            _producer = new ProducerBuilder<Null, byte[]>(_producerConfig)
                .SetErrorHandler(_errorHandler)
                .SetLogHandler((pro, msg) =>
                {
                    if (msg.Level <= SyslogLevel.Error)
                        Log.ForContext(SKIP_KEY, string.Empty).Error($"[Kafka] {msg.Level} {msg.Message}");
                    else
                        Log.ForContext(SKIP_KEY, string.Empty).Information($"[Kafka] {msg.Level} {msg.Message}");
                })
                .Build();
        }
    }
}