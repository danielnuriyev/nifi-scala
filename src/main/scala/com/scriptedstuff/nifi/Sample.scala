package com.scriptedstuff.nifi

import java.io.InputStream
import java.util
import java.util.List

import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor._

import scala.util.Try

/**
  * A processor that can be used for learning Nifi or for extending
  *
  * A processor must be added to in META-INF/services/org.apache.nifi.processor.Processor
  *
  * @author Daniel Nuriyev
  */
@EventDriven // allows the processor to process incoming flowfiles as they come in without waiting for a schedule to kick in
@CapabilityDescription("A processor that can be used for earning Nifi or for extending") // this is what is seen in the ui
class Sample extends Processor {

  // nifi use its own logging interface although slf4j works too (LoggerFactory.getLogger(getClass()))
  private var logger: ComponentLog = null

  /*
   relationships (aka connections) from this processor out
   i recommend the following connections:
    success - this is what this processor is supposed to output
      success should connect not only to processors that expect it but also to a logging processor
    failure - when a failure occurs.
      i recommend outputting an incoming flowfile and adding attributes that describe the failure such as:
        time,stacktrace,description
      failure processor should connect to a processors that
        allow replaying the flowfile
        handling the error such as sending an email
        logging the error

    in the ui a relationship is called a connection
    a relationship has a queue of flowfiles
  */
  final private val REL_SUCCESS: Relationship = new Relationship.Builder().name("success").build
  final private val REL_FAILURE: Relationship = new Relationship.Builder().name("failure").build

  // need a java collection because nifi is in java
  // it is  a common practice to differentiate between java collection, scala immutable collections, scala mutable collections:
  // util.Set, mutable.Set, immutable.Set
  private var relationships: util.Set[Relationship] = null

  override def initialize(context: ProcessorInitializationContext) {

    logger = context.getLogger() // alternatively slf4j: LoggerFactory.getLogger(getClass())

    relationships = new util.HashSet[Relationship]
    relationships.add(REL_SUCCESS)
    relationships.add(REL_FAILURE)
    relationships = util.Collections.unmodifiableSet(relationships)

  }

  override def getRelationships(): util.Set[Relationship] = relationships // a method that returns a field can be defined briefly

  /*
   this method does the work
   the main work is to
    read an incoming flowfile
    process its attributes and content
    output one or more flowfiles
    rollback and log in extreme cases
  */
  override def onTrigger(context: ProcessContext, sessionFactory: ProcessSessionFactory) {

    val session = sessionFactory.createSession()

    try {

      var flowFile = session.get()

      if(flowFile == null) {
        // a schedule kicked in but there are no flowfiles in the queue
        return
      }

      // a flowfile has attributes and content
      // attributes are textual key value pairs
      // content can be anything

      val attributes = flowFile.getAttributes()

      var error :Throwable  = null

      var content :String = null // content may be a byte array

      var in: InputStream = null
      try {

        in = session.read(flowFile)

        // I have commented out some code to reduce the dependencies to the bare minimum
        // content = IOUtils.toString(in) // or .toByteArray(in)

      // without this catch the error will be caught by the outer catch and the transaction will be rolled back
      // the goal of this catch is to set a specific error message and eventually write a flowfile to the 'failure' relationship
      } catch {
        case t :Throwable => {
          error = new Exception("Failed to read the flowfile", t)
        }
      } finally {
        Try(in.close)
      }

      if(error == null) {

        // PROCESS THE CONTENT

        /*
         Important:
         1. an incoming flowfile must be either removed or transferred before a session is committed
         2. any change to an flowfile results in a new flowfile and the old flowfile cannot be used
         */

        // if you don't transfer the incoming flowfile further, remove it
        // if you neither remove not transfer the incoming flowfile, you'll get an error
        session.remove(flowFile)

        // create a new flowfile with all the attributes (but not content of the incoming flowfile)
        flowFile = session.create(flowFile)
        // or session.create() if you don't want all the attributes of the incoming flowFile

        // you may need to add attru=ibutes to the flowfile
        flowFile = session.putAttribute(flowFile, "isThisAGoodExample", "sure")

        // write new content
        //val in = IOUtils.toInputStream("new content", "UTF-8")
        //flowFile = session.importFrom(in, flowFile)

        // output the flowfile to success
        session.transfer(flowFile, REL_SUCCESS)

      } else {

        val message = error.getMessage()
        // val stacktrace = ExceptionUtils.getStackTrace(error.getCause())

        logger.error(message, error.getCause())

        flowFile = session.putAttribute(flowFile, "error.message", message)
        // flowFile = session.putAttribute(flowFile, "error.stacktrace", stacktrace)

        session.transfer(flowFile, REL_FAILURE)

      }

      session.commit()

    } catch {
      case t: Throwable => {

        // val stack = ExceptionUtils.getStackFrames(t)
        // logger.error("Failed to run processor " + getClass().getSimpleName() + ": " + stack)

        session.rollback(true) // the incoming flowfile will stay unprocessed in teh queue (connection/relationship)

        throw t

      }
    }
  }

  override def getIdentifier: String = "Base"

  /*
   a processor may have properties that can be set in the ui
   methods below may be used for the properties
  */

  override def getPropertyDescriptors: List[PropertyDescriptor] = null

  override def getPropertyDescriptor(descriptorName: String): PropertyDescriptor = new PropertyDescriptor.Builder().name(descriptorName).build()

  override def onPropertyModified(propertyDescriptor: PropertyDescriptor, s: String, s1: String) {}

  override def validate(validationContext: ValidationContext): util.Collection[ValidationResult] = util.Collections.singleton(new ValidationResult.Builder().valid(true).build())

}
