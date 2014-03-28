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

		@max_s = 0
		@max_p = 0
		@max_sp = 0

		self.create_connection

		puts "Company #{@affiliate_id} initialized"
	end

	def extract()
		self.extract_max_ids

		self.extract_suppliers
		self.extract_parts
		self.extract_shipments

		self.close_connection

		puts "Company #{@affiliate_id} data extracted"
	end

	def transform()
		puts "Data of affiliate #{@affiliate_id} transformed"
	end

	def load()
		return if @suppliers == nil or @addresses == nil or @parts == nil or @shipments == nil
		return if @suppliers.count + @addresses.count + @parts.count + @shipments.count == 0

		@storage.query("INSERT INTO updates (time, affiliate_id, s, p, sp) \
				VALUES (UNIX_TIMESTAMP(), #{@affiliate_id}, #{@max_s}, #{@max_p}, #{@max_sp})")

		puts "Values for #{@affiliate_id} affiliate added to warehouse: suppliers (#{@last_s}, #{@max_s}), parts (#{@last_p}, #{@max_p}), relations (#{@last_sp}, #{@max_sp})"
	end
end

class AffiliateOne < Company
	def initialize(storage, affiliate_id, last_s, last_p, last_sp)
		super(storage, affiliate_id, last_s, last_p, last_sp)
	end

	protected
	def create_connection()
		begin
			@con = Mysql.new 'localhost', 'root', 'finncrisporiginal', 'company'			
		rescue Mysql::Error => e
			puts e.errno
			puts e.error
			self.close_connection
		end
	end

	protected
	def close_connection()
		@con.close if @con
	end

	protected
	def extract_max_ids()
		begin
			res = @con.query("SELECT s.max_id AS SID, p.max_id AS PID, sp.max_id AS SPID \
							FROM \
							(SELECT MAX(SID) AS max_id FROM S) s \
							JOIN (SELECT MAX(PID) AS max_id FROM P) p \
							JOIN (SELECT MAX(SPID) AS max_id FROM SP) sp").fetch_hash

			@max_s = res['SID']
			@max_p = res['PID']
			@max_sp = res['SPID']
		rescue Mysql::Error => e
			puts e.errno
			puts e.error
			self.close_connection
		end
	end

	protected
	def extract_suppliers()
		begin
			res = @con.query("SELECT * FROM S \
					WHERE SID BETWEEN #{@last_s} AND #{@max_s} AND \
					SName IS NOT NULL AND \
					SCity IS NOT NULL AND \
					Address IS NOT NULL")

			@suppliers = Array.new(res.num_rows)
			res.each_hash do |row|
				@suppliers << {:supplier => row["SName"], :city => row["SCity"], :address => row["Address"]}
			end
		rescue Mysql::Error => e
			puts e.errno
			puts e.error
			self.close_connection
		end
	end

	protected
	def extract_parts()
		begin
			res = @con.query("SELECT * FROM P \
					WHERE PID BETWEEN #{@last_p} AND #{@max_p} AND
					PName IS NOT NULL AND \
					Weight > 0")

			@parts = Array.new(res.num_rows)
			res.each_hash do |row|
				@parts << {:name => row["PName"], :htp => row["HTP"], :weight => row["Weight"]}
			end
		rescue Mysql::Error => e
			puts e.errno
			puts e.error
			self.close_connection
		end
	end

	protected
	def extract_shipments()
		begin
			res = @con.query("SELECT \
					S.SName AS Supplier, \
					P.PName AS Part, \
					SP.Qty AS Quantity, \
					SP.OrderDate AS OrderDate, \
					SP.Period AS Period, \
					SP.ShipDate AS ShipDate, \
					SP.Price AS Price, \
					P.Weight * SP.Qty AS SPWeight \
				FROM SP \
				INNER JOIN S ON S.SID = SP.SID \
				INNER JOIN P ON P.PID = SP.PID \
				WHERE \
					SP.SPID BETWEEN #{@last_sp} AND #{@max_sp} AND \
					S.SName IS NOT NULL AND \
					S.SCity IS NOT NULL AND \
					S.Address IS NOT NULL AND \
					P.PName IS NOT NULL AND \
					P.Weight > 0 AND \
					SP.OrderDate IS NOT NULL AND \
					SP.Qty > 0 AND \
					SP.Price > 0 AND \
					SP.Period >= 0 AND \
					SP.OrderDate <= SP.ShipDate \
				HAVING SPWeight BETWEEN 0 AND 1500")

			@shipments = Array.new(res.num_rows)
			res.each_hash do |row|
				@shipments << {:supplier => row["Supplier"], :part => row["Part"], :qty => row["Quantity"], 
								:order_date => row["OrderDate"], :ship_date => row["ShipDate"], :period => row["Period"], 
								:weight => row["Weight"], :price => row["Price"]}
			end
		rescue Mysql::Error => e
			puts e.errno
			puts e.error
			self.close_connection
		end
	end
end

class AffiliateTwo < Company
	def initialize(storage, affiliate_id, last_s, last_p, last_sp)
		super(storage, affiliate_id, last_s, last_p, last_sp)
	end

	protected
	def create_connection()
	end

	protected
	def close_connection()
	end

	protected
	def extract_max_ids()
	end

	protected
	def extract_suppliers()
	end

	protected
	def extract_parts()
	end
	
	protected
	def extract_shipments()
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
			affiliate = database.new(storage, i, last_ids[i][:last_sid], last_ids[i][:last_pid], last_ids[i][:last_spid])

			affiliate.extract()
			affiliate.transform()
			affiliate.load()
		}
	}
	threads.each {|t| t.join}

	storage.close

	puts 'End ETL process'
end