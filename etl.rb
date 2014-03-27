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
		begin
			con = Mysql.new 'localhost', 'root', 'finncrisporiginal', 'company'

			res = con.query("SELECT s.max_id AS SID, p.max_id AS PID, sp.max_id AS SPID \
						FROM \
						(SELECT MAX(SID) AS max_id FROM S) s \
						JOIN (SELECT MAX(PID) AS max_id FROM P) p \
						JOIN (SELECT MAX(SPID) AS max_id FROM SP) sp").fetch_hash

			max_S = res['SID']
			max_P = res['PID']
			max_SP = res['SPID']

			res = con.query("SELECT \
								S.SName AS Supplier, \
								P.PName AS Part, \
								SP.Qty AS Quantity, \
								SP.OrderDate AS OrderDate, \
								SP.Period AS Period, \
								SP.ShipDate AS ShipDate, \
								S.Risk AS Risk, \
								P.HTP AS HTP, \
								SUM(P.Weight) AS SPWeight \
							FROM SP \
							INNER JOIN S ON S.SID = SP.SID \
							INNER JOIN P ON P.PID = SP.PID \
							WHERE \
								S.SID BETWEEN #{@last_s} AND #{max_S} AND \
								P.PID BETWEEN #{@last_p} AND #{max_P} AND \
								SP.SPID BETWEEN #{@last_sp} AND #{max_SP} AND \
								S.SName IS NOT NULL AND \
								S.SCity IS NOT NULL AND \
								S.Address IS NOT NULL AND \
								P.PName IS NOT NULL AND \
								P.Weight > 0 AND \
								P.HTP IN (0, 1) AND \
								SP.OrderDate IS NOT NULL AND \
								SP.Qty > 0 AND \
								SP.Price > 0 AND \
								SP.Period >= 0 AND \
								SP.OrderDate <= SP.ShipDate \
							HAVING SPWeight <= 1500")

			puts "Values for #{@affiliate_id} are added to warehouse: suppliers (#{@last_s}, #{max_S}), parts (#{@last_p}, #{max_P}), relations (#{@last_sp}, #{max_SP})"

			@last_s = max_S
			@last_p = max_P
			@last_sp = max_SP

			self.load()
		rescue Mysql::Error => e
			puts e.errno
			puts e.error
		ensure
			con.close if con
		end
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