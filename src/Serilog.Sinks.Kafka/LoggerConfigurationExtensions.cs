using System;
using Confluent.Kafka;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.Kafka
{
    public static class LoggerConfigurationExtensions
    {
        /// <summary>
        /// Adds a sink that writes log events to a Kafka topic in the broker endpoints.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="batchSizeLimit">The maximum number of events to include in a single batch.</param>
        /// <param name="period">The time in seconds to wait between checking for event batches.</param>
        /// <param name="bootstrapServers">The list of bootstrapServers separated by comma.</param>
        /// <param name="errorHandler">kafka errorHandler</param>
        /// <param name="topic">The topic name.</param>
        /// <returns></returns>
        public static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            string bootstrapServers = "localhost:9092",
            int batchSizeLimit = 50,
            int period = 5,
            SecurityProtocol securityProtocol = SecurityProtocol.Plaintext,
            SaslMechanism? saslMechanism = null,
            string topic = "logs",
            string saslUsername = null,
            string saslPassword = null,
            string sslCaLocation = null,
            string messageKey = null,
            Action<IProducer<string, byte[]>, Error> errorHandler = null,
            ITextFormatter formatter = null,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            LoggingLevelSwitch? levelSwitch = null) 
        {
            return loggerConfiguration.Kafka(
                bootstrapServers,
                batchSizeLimit,
                period,
                securityProtocol,
                saslMechanism,
                saslUsername,
                saslPassword,
                sslCaLocation,
                topic,
                topicDecider: null,
                messageKey,
                errorHandler,
                formatter, 
                restrictedToMinimumLevel,
                levelSwitch);
        }

        /// <summary>
        /// Adds a sink that writes log events to a Kafka topic in the broker endpoints.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="batchSizeLimit">The maximum number of events to include in a single batch.</param>
        /// <param name="period">The time in seconds to wait between checking for event batches.</param>
        /// <param name="bootstrapServers">The list of bootstrapServers separated by comma.</param>
        /// <param name="errorHandler">kafka errorHandler</param>
        /// <param name="topic">The topic name.</param>
        /// <returns></returns>
        public static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            Func<LogEvent, string> topicDecider,
            string bootstrapServers = "localhost:9092",
            int batchSizeLimit = 50,
            int period = 5,
            SecurityProtocol securityProtocol = SecurityProtocol.Plaintext,
            SaslMechanism? saslMechanism = null,
            string saslUsername = null,
            string saslPassword = null,
            string sslCaLocation = null,
            string messageKey = null,
            Action<IProducer<string, byte[]>, Error> errorHandler = null,
            ITextFormatter formatter = null,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            LoggingLevelSwitch? levelSwitch = null) 
        {
            return loggerConfiguration.Kafka(
                bootstrapServers,
                batchSizeLimit,
                period,
                securityProtocol,
                saslMechanism,
                saslUsername,
                saslPassword,
                sslCaLocation,
                topic: null,
                topicDecider,
                messageKey,
                errorHandler,
                formatter,
                restrictedToMinimumLevel, 
                levelSwitch);
        }

        public static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            string topic,
            Func<LogEvent, string> topicDecider,
            ProducerConfig producerConfig,
            int batchSizeLimit = 50,
            int period = 5,
            string messageKey = null,
            Action<IProducer<string, byte[]>, Error> errorHandler = null,
            ITextFormatter formatter = null,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            LoggingLevelSwitch? levelSwitch = null) 
        {
            return loggerConfiguration.Kafka(producerConfig, topic, batchSizeLimit, period, topicDecider, messageKey, 
                errorHandler, formatter, restrictedToMinimumLevel, levelSwitch);
        }

        private static LoggerConfiguration Kafka(
            this LoggerSinkConfiguration loggerConfiguration,
            string bootstrapServers,
            int batchSizeLimit,
            int period,
            SecurityProtocol securityProtocol,
            SaslMechanism? saslMechanism,
            string saslUsername,
            string saslPassword,
            string sslCaLocation,
            string topic,
            Func<LogEvent, string> topicDecider,
            string messageKey,
            Action<IProducer<string, byte[]>, Error> errorHandler,
            ITextFormatter formatter,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            LoggingLevelSwitch? levelSwitch = null)
        {
            return loggerConfiguration.Kafka(new ProducerConfig()
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = securityProtocol,
                SaslMechanism = saslMechanism,
                SaslUsername = saslUsername,
                SaslPassword = saslPassword,
                SslCaLocation = sslCaLocation
            }, topic, batchSizeLimit, period, topicDecider, messageKey, errorHandler, formatter,
            restrictedToMinimumLevel, levelSwitch);
        }

        private static LoggerConfiguration Kafka(
           this LoggerSinkConfiguration loggerConfiguration,
           ProducerConfig producerConfig,
           string topic,
           int batchSizeLimit,
           int period,
           Func<LogEvent, string> topicDecider,
           string messageKey,
           Action<IProducer<string, byte[]>, Error> errorHandler,
           ITextFormatter formatter,
           LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
           LoggingLevelSwitch? levelSwitch = null) 
        {
            return ConfigureKafka(loggerConfiguration.Sink, formatter, producerConfig, topic, batchSizeLimit, 
                period, topicDecider, messageKey, errorHandler, restrictedToMinimumLevel, levelSwitch);
        }

        static LoggerConfiguration ConfigureKafka(
            this Func<ILogEventSink, LogEventLevel, LoggingLevelSwitch?, LoggerConfiguration> addSink,
            ITextFormatter formatter,
            ProducerConfig producerConfig,
            string topic,
            int batchSizeLimit,
            int period,
            Func<LogEvent, string> topicDecider,
            string messageKey,
            Action<IProducer<string, byte[]>, Error> errorHandler,
            LogEventLevel restrictedToMinimumLevel,
            LoggingLevelSwitch? levelSwitch) 
        {
            if (addSink == null) throw new ArgumentNullException(nameof(addSink));
            
            var kafkaSink = new KafkaSink(
                producerConfig,
                topic,
                topicDecider,
                formatter, messageKey, errorHandler);

            var batchingOptions = new PeriodicBatchingSinkOptions
            {
                BatchSizeLimit = batchSizeLimit,
                Period = TimeSpan.FromSeconds(period)
            };

            var batchingSink = new PeriodicBatchingSink(
                kafkaSink,
                batchingOptions);

            return addSink(batchingSink, restrictedToMinimumLevel, levelSwitch);
        }
    }
}
