#!/usr/bin/ruby

require 'mysql'
# require 'sqlite3'
require 'thread'


class AffiliateOne 
	def initialize()
		puts 'Company 1 initialized'
	end

	def extract()
		puts 'Company 1 data extracted'
	end
end

class AffiliateTwo
	def initialize()
		puts 'Company 2 initialized'
	end

	def extract()
		puts 'Company 2 data extracted'		
	end
end

begin
	puts 'Start ETL process'

	storage = Mysql.new 'localhost', 'root', 'finncrisporiginal', 'warehouse'
	res = storage.query('SELECT * FROM updates GROUP BY affiliate_id HAVING time = MAX(time)')

	databases = [AffiliateOne, AffiliateTwo]

	last_ids = Array.new(databases.count) {[{'last_sid' => 0, 'last_pid' => 0, 'last_spid' => 0}]}

	res.num_rows do |i|
		row = res.fetch_row
		if (affiliate_id = row['affiliate_id'].to_i > databases.count)
			puts "Too big 'affiliate_id' value (#{affiliate_id}) in 'updates' table"
			next
		end

		last_ids[i] = [{'last_sid' => row['s'], 'last_pid' => row['p'], 'last_spid' => row['sp']}]
	end

	puts last_ids

	threads = []
	databases.each_with_index {|database, i|
		threads << Thread.new() {
			database.new().extract()
		}
	}
	threads.each {|t| t.join}

	storage.close

	puts 'End ETL process'
end