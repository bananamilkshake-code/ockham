#!/usr/bin/ruby

require 'mysql'
# require 'sqlite3'
require 'thread'
require 'bigdecimal'

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

		puts "Company #{@affiliate_id} initialized"
	end

	def extract()
		self.create_connection

		self.extract_max_ids

		self.extract_suppliers
		self.extract_parts
		self.extract_shipments

		self.close_connection

		puts "Company #{@affiliate_id} data extracted"
	end

	def transform()
		self.create_suppliers
		self.create_parts
		self.create_shipments

		puts "Data of affiliate #{@affiliate_id} transformed"
	end

	def load()
		return if @shipments_query == nil
		puts @shipments_query
		@storage.query(@shipments_query)
		@storage.query("INSERT INTO updates (time, affiliate_id, s, p, sp) VALUES (UNIX_TIMESTAMP(), #{@affiliate_id}, #{@max_s}, #{@max_p}, #{@max_sp})")

		puts "Values for #{@affiliate_id} affiliate added to warehouse: suppliers (#{@last_s}, #{@max_s}), parts (#{@last_p}, #{@max_p}), relations (#{@last_sp}, #{@max_sp})"
	end

	###
	### Transform methods
	###
	protected
	def create_suppliers()
		return if @suppliers.empty?

		suppliers_values = String.new
		@suppliers.each do |supplier|
			suppliers_values << ',' if not suppliers_values.empty?
			suppliers_values << '("'	<< supplier[:name] << '","' << supplier[:address] << '","' << supplier[:city] << '")'
		end

		@storage.query ("INSERT IGNORE INTO suppliers(name, address, city) VALUES" + suppliers_values)
	end

	protected	
	def create_parts()
		return if @parts.empty?

		parts_values = String.new
		@parts.each do |part|
			parts_values << ',' if not parts_values.empty?
			parts_values << '("'	<< part[:name] << '",' << part[:htp] << ',' << part[:weight] << ')'
		end

		@storage.query "INSERT IGNORE INTO parts(name, HTP, weight) VALUES" + parts_values
	end

	protected	
	def create_shipments()
		return if @shipments.empty?

		suppliers_values = String.new
		parts_values = String.new
		@shipments.each do |shipment|
			suppliers_values << ',' if not suppliers_values.empty?
			suppliers_values << '("'	<< shipment[:supplier] << '","' << shipment[:address] << '","' << shipment[:city] << '")'

			parts_values << ',' if not parts_values.empty?
			parts_values << '("' << shipment[:part] << '",' << shipment[:part_weight] << ')'

		end

		supplier_ids = @storage.query('SELECT id, name, address, city FROM suppliers WHERE (name, address, city) IN (' + suppliers_values + ')')

		sids = Hash.new
		supplier_ids.each_hash do |supplier|
			city = supplier['city']
			name = supplier['name']
			address = supplier['address']
			id = supplier['id']

			sids[city] = Hash.new unless sids.key? city 
			by_city = sids[city]

			by_city[name] = Hash.new unless by_city.key? name
			by_name = by_city[name]

			by_name[address] = id
		end

		parts_ids = @storage.query('SELECT id, name, CAST(weight AS DECIMAL(8,4)) as weight FROM parts WHERE (name, CAST(weight AS DECIMAL(8,4))) IN (' + parts_values + ')')

		pids = Hash.new
		parts_ids.each_hash do |part|
			name = part["name"]
			weight = part["weight"]
			id = part["id"]

			pids[name] = Hash.new if not pids.key? name
			by_name = pids[name]

			by_name[weight] = id
		end

		@shipments.each do |shipment|
			shipment[:supplier_id] = sids[shipment[:city]][shipment[:supplier]][shipment[:address]]
			shipment[:part_id] = pids[shipment[:part]][shipment[:part_weight]]
		end

		shipments_values = String.new
		@shipments.each do |shipment|
			shipments_values << ',' if not shipments_values.empty?
			shipments_values << '('	<< shipment[:supplier_id].to_s << ',' << shipment[:part_id] << ',' \
								<< shipment[:qty] << ',' << shipment[:price] << ',' << shipment[:weight] << ',' \
								<< shipment[:order_date] << ',' << shipment[:period] << ',' << shipment[:ship_date] << ')'
		end

		@shipments_query = "INSERT INTO shipments(sid, pid, qty, price, weight, order_date, period, ship_date) VALUES" + shipments_values
	end
end

class AffiliateOne < Company
	def initialize(storage, affiliate_id, last_s, last_p, last_sp)
		super(storage, affiliate_id, last_s, last_p, last_sp)
	end

	protected
	def create_connection()
		begin
			@con = Mysql.init
			@con.options(Mysql::SET_CHARSET_NAME, 'utf8')
			@con.real_connect('localhost', 'root', 'finncrisporiginal', 'company')
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

	###
	### Extraction methods
	###

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
					WHERE SID BETWEEN #{@last_s} + 1 AND #{@max_s} AND \
					SName IS NOT NULL AND \
					SCity IS NOT NULL AND \
					Address IS NOT NULL")

			@suppliers = Array.new
			res.each_hash do |row|
				@suppliers << {:name => row["SName"], :city => row["SCity"], :address => row["Address"]}
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
			res = @con.query("SELECT PName, HTP, CAST(Weight AS DECIMAL(8,4)) AS Weight FROM P \
					WHERE PID BETWEEN #{@last_p} + 1 AND #{@max_p} AND
					PName IS NOT NULL AND \
					Weight > 0")

			@parts = Array.new
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
					S.Address AS Address, \
					S.SCity AS City, \
					P.PName AS Part, \
					CAST(P.Weight AS DECIMAL(8,4)) AS PartWeight, \
					SP.Qty AS Quantity, \
					UNIX_TIMESTAMP(SP.OrderDate) AS OrderDate, \
					SP.Period AS Period, \
					UNIX_TIMESTAMP(SP.ShipDate) AS ShipDate, \
					SP.Price AS Price, \
					P.Weight * SP.Qty AS SPWeight \
				FROM SP \
				INNER JOIN S ON S.SID = SP.SID \
				INNER JOIN P ON P.PID = SP.PID \
				WHERE \
					SP.SPID BETWEEN #{@last_sp} + 1 AND #{@max_sp} AND \
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

			@shipments = Array.new
			res.each_hash do |row|
				@shipments << {:supplier => row["Supplier"], :city => row["City"], :address => row["Address"], :supplier_id => 0,
								:part => row["Part"], :part_weight => row["PartWeight"], :qty => row["Quantity"], :part_id => 0,
								:order_date => row["OrderDate"], :ship_date => row["ShipDate"], :period => row["Period"], 
								:weight => row["SPWeight"], :price => row["Price"]}
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

	###
	### Extraction methods
	###

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
		AffiliateOne
	]

	last_ids = Array.new(databases.count) {{:last_sid => 1, :last_pid => 1, :last_spid => 1}}

	storage = Mysql.init
	storage.options(Mysql::SET_CHARSET_NAME, 'utf8')
	storage.real_connect('localhost', 'root', 'finncrisporiginal', 'warehouse')

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