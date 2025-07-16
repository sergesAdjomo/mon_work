from unittest.mock import Mock
from consumer.consumer import ConsumerCiam
import logging

def test_kafka_exploration(spark_session):

    # Given 

    # When
    consumer = ConsumerCiam(spark_session, config=Mock())
    consumer.consume()
    
    # Then
    

