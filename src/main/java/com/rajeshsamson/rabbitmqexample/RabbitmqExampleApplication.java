package com.rajeshsamson.rabbitmqexample;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.util.Date;

@SpringBootApplication
public class RabbitmqExampleApplication {

  static final String topicExchangeName = "My-Topic";

  static final String queueName = "My-Queue";

  @Bean
  Queue queue() {
    return new Queue(queueName, false);
  }

  @Bean
  TopicExchange exchange() {
    return new TopicExchange(topicExchangeName);
  }

  @Bean
  Binding binding(Queue queue, TopicExchange exchange) {
    return BindingBuilder.bind(queue).to(exchange).with("foo.bar.#");
  }

  @Bean
  SimpleMessageListenerContainer container(ConnectionFactory connectionFactory, MessageListenerAdapter listenerAdapter) {
    SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
    container.setConnectionFactory(connectionFactory);
    container.setQueueNames(queueName);
    container.setMessageListener(listenerAdapter);
    return container;
  }

  @Bean
  MessageListenerAdapter listenerAdapter(Receiver receiver) {
    return new MessageListenerAdapter(receiver, "receiveMessage");
  }

  public static void main(String[] args) {

    SpringApplication.run(RabbitmqExampleApplication.class, args);
  }
}

@RestController
class MessageResource {

  Receiver receiver;
  RabbitTemplate rabbitTemplate;

  MessageResource(Receiver receiver, RabbitTemplate rabbitTemplate) {
    this.receiver = receiver;
    this.rabbitTemplate = rabbitTemplate;
  }

  @PostMapping("/rabbit")
  public ResponseEntity<Message> pushMessage(@RequestBody Message message) {
    rabbitTemplate.convertAndSend("My-Topic", "foo.bar.baz", message);
    return new ResponseEntity<Message>(message, HttpStatus.OK);
  }
}

@Component
class Receiver {

  public void receiveMessage(Message message) {
    System.out.println("Received <" + message + ">");
  }
}

class Message implements Serializable {

	String msg;
	Date createdDate;

	public Message() {
		createdDate = new Date();
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	@Override
	public String toString() {
		return "Message{" +
				"msg = '" + msg + '\'' +
				", createdDate = " + createdDate +
				'}';
	}
}
