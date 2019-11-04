package mflix.api.daos;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.session.BaseClientSessionImpl;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.text.MessageFormat;
import java.util.Map;

import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

  private final MongoCollection<User> usersCollection;
  //TODO> Ticket: User Management - do the necessary changes so that the sessions collection
  //returns a Session object
  private final MongoCollection<Session> sessionsCollection;

  private final Logger log;

  @Autowired
  public UserDao(
          MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
    super(mongoClient, databaseName);
    CodecRegistry pojoCodecRegistry =
            fromRegistries(
                    MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().automatic(true).build()));

    usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
    log = LoggerFactory.getLogger(this.getClass());
    //<TODO> Ticket: User Management - implement the necessary changes so that the sessions
    // collection returns a Session objects instead of Document objects.
    sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
  }

  /**
   * Inserts the `user` object in the `users` collection.
   *
   * @param user - User object to be added
   * @return True if successful, throw IncorrectDaoOperation otherwise
   */
  public boolean addUser(User user) {
    try {
      usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user);
      return true;
    } catch (MongoException ex) {
      throw new IncorrectDaoOperation("Addition of user failed");
    }
  }

  /**
   * Creates session using userId and jwt token.
   *
   * @param userId - user string identifier
   * @param jwt - jwt string token
   * @return true if successful
   */
  public boolean createUserSession(String userId, String jwt) {
    try {
      Bson updateFilter = new Document("user_id", userId);
      Bson setUpdate = set("jwt", jwt);
      UpdateOptions options = new UpdateOptions().upsert(true);
      sessionsCollection.updateOne(updateFilter, setUpdate, options);
      return true;
    } catch (MongoException ex) {
      return false;
    }
  }

  /**
   * Returns the User object matching the an email string value.
   *
   * @param email - email string to be matched.
   * @return User object or null.
   */
  public User getUser(String email) {
    try {
      Document user = new Document("email", email);
      return usersCollection.find(user).limit(1).first();
    } catch (MongoException ex) {
      return null;
    }
  }

  /**
   * Given the userId, returns a Session object.
   *
   * @param userId - user string identifier.
   * @return Session object or null.
   */
  public Session getUserSession(String userId) {
    try {
      Document session = new Document("user_id", userId);
      return sessionsCollection.find(session).limit(1).first();
    } catch (MongoException ex) {
      return null;
    }
  }

  public boolean deleteUserSessions(String userId) {
    try {
      Document session = new Document("user_id", userId);
      DeleteResult dr = sessionsCollection.deleteOne(session);
      if(dr.getDeletedCount() < 1) {
        log.warn("User `{}`, could not be found in sessions collection.", userId);
      }
      return dr.wasAcknowledged();
    } catch (MongoException ex) {
      return false;
    }
  }

  /**
   * Removes the user document that match the provided email.
   *
   * @param email - of the user to be deleted.
   * @return true if user successfully removed
   */
  public boolean deleteUser(String email) {
    try {
      Document user = new Document("email", email);
      Document session = new Document("user_id", email);
      usersCollection.deleteOne(user);
      sessionsCollection.deleteMany(session);
      return true;
    } catch (MongoException ex) {
      return false;
    }
  }

  /**
   * Updates the preferences of an user identified by `email` parameter.
   *
   * @param email - user to be updated email
   * @param userPreferences - set of preferences that should be stored and replace the existing
   *     ones. Cannot be set to null value
   * @return User object that just been updated.
   */
  public User updateUserPreferences(String email, Map<String, String> userPreferences) {

    if(userPreferences == null){
      throw new IncorrectDaoOperation("userPreferences cannot be set to null");
    }

    try {
      // create query filter and update object.
      Bson updateFilter = new Document("email", email);
      Bson updateObject = Updates.set("preferences", userPreferences);
      UpdateResult updateResult = usersCollection.updateOne(updateFilter, updateObject);
      if(updateResult.getModifiedCount() < 1) {
        log.warn("User `{}` was not updated. Trying to re-write the same `preferences` field: `{}`", email, userPreferences);
        return null;
      }
      Bson findFilter = new Document("_id",  new ObjectId(updateResult.getUpsertedId().toString()));
      return usersCollection.find(findFilter).limit(1).first();
    } catch(MongoException me) {
      me.printStackTrace();
      return null;
    }
  }
}
