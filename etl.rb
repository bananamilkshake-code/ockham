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

	protected
	def load()
		@storage.query("INSERT INTO updates (time, affiliate_id, s, p, sp) \
				VALUES (UNIX_TIMESTAMP(), #{@affiliate_id}, #{@last_s}, #{@last_p}, #{@last_sp})")
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

	last_ids = Array.new(databases.count) {{:last_sid => 1, :last_pid => 1, :last_spid => 1}}

	storage = Mysql.new 'localhost', 'root', 'finncrisporiginal', 'warehouse'

	res = storage.query("SELECT u.affiliate_id AS affiliate_id, u.s AS s, u.p AS p, u.sp AS sp
						FROM updates u
						INNER JOIN (SELECT affiliate_id, MAX(time) AS time FROM updates GROUP BY affiliate_id) max_time 
							ON u.time = max_time.time AND u.affiliate_id = max_time.affiliate_id")

	res.each_hash do |row|
		affiliate_id = row['affiliate_id'].to_i

		if affiliate_id >= databases.count
			puts "Too big 'affiliate_id' value (#{affiliate_id}) in 'updates' table"
			next
		end

		last_ids[affiliate_id][:last_sid] = row['s'].to_i
		last_ids[affiliate_id][:last_pid] = row['p'].to_i
		last_ids[affiliate_id][:last_spid] = row['sp'].to_i
	end

	threads = []
	databases.each_with_index {|database, i|
		threads << Thread.new() {
			database.new(storage, i, last_ids[i][:last_sid], last_ids[i][:last_pid], last_ids[i][:last_spid]).extract()
		}
	}
	threads.each {|t| t.join}

	storage.close

	puts 'End ETL process'
end