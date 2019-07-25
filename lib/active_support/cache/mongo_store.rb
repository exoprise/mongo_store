require 'active_support'
require 'mongo'

module MongoStore
  module Cache

    module Rails3
      def write_entry(key, entry, options)
        expires = Time.now + options[:expires_in]
        value = entry.value
        value = value.to_mongo if value.respond_to? :to_mongo
        begin
          collection.update_one({'_id' => key}, {'$set' => {'value' => value, 'expires' => expires}}, :upsert => true)
        rescue Mongo::Error::InvalidDocument
          value = value.to_s and retry unless value.is_a? String
        end
      end
      def read_entry(key, options=nil)
        doc = collection.find('_id' => key, 'expires' => {'$gt' => Time.now}).limit(1).first
        ActiveSupport::Cache::Entry.new(doc['value']) if doc
      end
      def delete_entry(key, options=nil)
        #collection.remove({'_id' => key})
        collection.delete_many({'_id' => key})
      end
      def delete_matched(pattern, options=nil)
        options = merged_options(options)
        instrument(:delete_matched, pattern.inspect) do
          matcher = key_matcher(pattern, options)  # Handles namespacing with regexes
          delete_entry(matcher, options) 
        end
      end
    end
    
    module Store
      include Rails3
      
      def expires_in
        options[:expires_in]
      end
      
      def expires_in=(val)
        options[:expires_in] = val
      end
    end
  end
end

module ActiveSupport
  module Cache
    class MongoStore < Store
      include ::MongoStore::Cache::Store
      
      # Returns a MongoDB cache store.  Can take either a Mongo::Collection object or a collection name.
      # If neither is provided, a collection named "rails_cache" is created.
      #
      # An options hash may also be provided with the following options:
      # 
      # * :expires_in - The default expiration period for cached objects. If not provided, defaults to 1 day.
      # * :db - Either a Mongo::DB object or a database name. Not used if a Mongo::Collection object is passed. Otherwise defaults to MongoMapper.database (if MongoMapper is used in the app) or else creates a DB named "rails_cache".
      # * :create_index - Whether to index the key and expiration date on the collection. Defaults to true. Not used if a Mongo::Collection object is passed.
      def initialize(collection = nil, options = nil)
        @options = {
          :collection_name => 'rails_cache',
          :db_name => 'rails_cache',
          :expires_in => 86400,  # That's 1 day in seconds
          :create_index => true
        }
        # @options.merge!(options) if options
        case collection
        when Mongo::Collection
          @collection = collection
        when String
          @options[:collection_name] = collection
        when Hash
          @options.merge!(collection)
        when nil
          # No op
        else
          raise TypeError, "MongoStore parameters must be a Mongo::Collection, a collection name, and/or an options hash."
        end
        
        @options.merge!(options) if options.is_a?(Hash)
      end
      
      # Returns the MongoDB collection described in the options to .new (or else the default 'rails_cache' one.)
      # Lazily creates the object on first access so that we can look for a MongoMapper database _after_ 
      # MongoMapper initializes.
      def collection
        @collection ||= make_collection
      end
            
      # Removes old cached values that have expired.  Set this up to run occasionally in delayed_job, etc., if you
      # start worrying about space.  (In practice, because we favor updating over inserting, space is only wasted
      # if the key itself never gets cached again.  It also means you can _reduce_ efficiency by running this
      # too often.)
      def clean_expired
        #collection.remove({'expires' => {'$lt' => Time.now}})
        collection.delete_many({'expires' => {'$lt' => Time.now}})
      end
      
      # Wipes the whole cache.
      def clear
        #collection.remove
        collection.delete_many
      end


      #basically lifted from memory_store. not clear to me my there isn't a default implementation
      def increment(name, amount = 1, options = nil)
        options = merged_options(options)
        if num = read(name, options)
          num = num.to_i + amount
          write(name, num, options)
          num
        else
          nil
        end
      end


      def decrement(name, amount =1, options = nil)
        options = merged_options(options)
        if num = read(name, options)
          num = num.to_i - amount
          write(name, num, options)
          num
        else
          nil
        end
      end
      
      private
      def mongomapper?
        Kernel.const_defined?(:MongoMapper) && MongoMapper.respond_to?(:database) && MongoMapper.database
      end
      
      def make_collection
        # db = case options[:db]
        # when Mongo::DB then options[:db]
        # when String then Mongo::DB.new(options[:db], Mongo::Connection.new)
        # else
        #   if mongomapper?
        #     MongoMapper.database
        #   else
        #     Mongo::Database.new(options[:db_name], Mongo::Connection.new)
        #   end
        # end
        db = options[:db]
        # coll = db.create_collection(options[:collection_name])
        # coll.create_index([['_id',Mongo::ASCENDING], ['expires',Mongo::DESCENDING]]) if options[:create_index]
        coll = db[options[:collection_name]]
        coll.indexes.create_one({_id: 1, expires: -1}) if options[:create_index]
        coll
      end
        
    end
  end
end
