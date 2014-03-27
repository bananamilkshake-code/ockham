#!/usr/bin/ruby

require 'mysql'
# require 'sqlite3'
require 'thread'

class Company
	def initialize(storage, affiliate_id, last_s, last_p, last_sp)
		@storage = storage
		@affiliate_id = affiliate_id

		@last_s = last_s
		@last_p = last_p
		@last_sp = last_sp
	end

	def load()
		storage.query('INSERT INTO updates (time, affiliate_id, s, p, sp) \
						VALUES (UNIX_TIMESTAMP(), #{@affiliate_id}, #{@last_s}. #{@last_p}, #{@last_sp})')
	end
end

class AffiliateOne < Company
	def initialize(storage, affiliate_id, last_s, last_p, last_sp)
		super(storage, affiliate_id, last_s, last_p, last_sp)
		puts 'Company 1 initialized'
	end

	def extract()
		puts 'Company 1 data extracted'
	end
end

class AffiliateTwo < Company
	def initialize(storage, affiliate_id, last_s, last_p, last_sp)
		super(storage, affiliate_id, last_s, last_p, last_sp)
		puts 'Company 2 initialized'
	end

	def extract()
		puts 'Company 2 data extracted'		
	end
end

begin
	puts 'Start ETL process'

	databases = [
		AffiliateOne, 
		AffiliateTwo
	]

	last_ids = Array.new(databases.count) {{'last_sid' => 0, 'last_pid' => 0, 'last_spid' => 0}}

	storage = Mysql.new 'localhost', 'root', 'finncrisporiginal', 'warehouse'

	res = storage.query('SELECT * FROM updates GROUP BY affiliate_id HAVING time = MAX(time)')
	res.num_rows do |i|
		row = res.fetch_row
		if (affiliate_id = row['affiliate_id'] > databases.count)
			puts "Too big 'affiliate_id' value (#{affiliate_id}) in 'updates' table"
			next
		end

		last_ids[i] = {'last_sid' => row['s'], 'last_pid' => row['p'], 'last_spid' => row['sp']}
	end

	threads = []
	databases.each_with_index {|database, i|
		threads << Thread.new() {
			database.new(storage, i, last_ids[i]['last_sid'], last_ids[i]['last_pid'], last_ids[i]['last_spid']).extract()
		}
	}
	threads.each {|t| t.join}

	storage.close

	puts 'End ETL process'
end