MySQL DB Patterns
=================
by [Andrew Brampton](http://bramp.net) 2013, contributed by [Mateusz Zakarczemny](https://github.com/Matzz)

Intro
-----

Some useful java code backed by JDBC that implements some common patterns.

So far a Condition object, and a Queue are implemented.

```maven
	<dependency>
		<groupId>net.bramp.db-patterns</groupId>
		<artifactId>db-patterns</artifactId>
		<version>0.1</version>
	</dependency>
```

Condition
---------

A distributed Java Condition

```java
  DataSource ds = ...
  Condition condition = new MySQLSleepBasedCondition(ds, "lockname");
  condition.await(); // Blocks until a notify
  
  // on another thread (or process, or machine)
  condition.notify();
```

The MySQLSleepBasedCondition is based on the MySQL ``SLEEP()`` and ``KILL QUERY``

The thread that is woken up is guaranteed to be the one that has waited the longest.


Blocking queue
-----

A distributed MySQL backed Java BlockingQueue

```java
  DataSource ds = ...
  
  //datasoruce, queue table, queuename, value type, thread name
  BlockingQueue<String> queue = new MySQLBasedQueue<String>(ds, "queue", "queue name", String.class, "Worker1");
  queue.add("Some String");
  
  // on another thread (or process, or machine)
  String s = queue.poll(); // Non blocking
  // or
  String s = queue.take(); // Blocks until element available
```

The MySQLBasedQueue uses the MySQLSleepBasedCondition to help form a blocking
queue, that can work without polling the database for new work.

More complex types could be stored using serializator:
```java
  Serializator serializator = new DefaultSerializator<MyType>();
  BlockingQueue<String> queue = new MySQLBasedQueue<String>(ds, "queue", "queue name", serializator, "Worker1");
  MyType value = new MyType(...);
  queue.add(value);
```
DefaultSerializator serializes values using java ObjectOutputStream but other implementation might be passed to queue (eg. some custom JsonSerializer).

DelayQueue
-----------------
A distributed MySQL backed Java DelayQueue

```java
  Serializator serializator = new DefaultSerializator<MyDelayedType>(); // MyType must extends Delayed interface
  DelayQueue<String> queue = new MySQLBasedDelayQueue<String>(ds, "queue", "queue name", serializator, "Worker1");
  MyDelayedType value = new MyDelayedType(10, TimeUnit.SECONDS);
  queue.add(value);
  queue.peek(); // equals null
  Thread.sleep(11*1000);
  queue.peek(); // equals value
  
```



Build and Release
-----------------

To build this project use `mvn`.

To push a release to maven central use the standard maven release plugin, and Sonatype's OSS repo:

```bash
mvn release:prepare
mvn release:perform
```

Useful Articles
---------------
	https://blog.engineyard.com/2011/5-subtle-ways-youre-using-mysql-as-a-queue-and-why-itll-bite-you
