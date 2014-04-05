#!/usr/bin/ruby

require 'mysql'
require 'sqlite3'
require 'thread'
require 'bigdecimal'

class Company
	@@query_max_ids = "SELECT s.max_id AS SID, p.max_id AS PID, sp.max_id AS SPID \
			FROM \
			(SELECT MAX(SID) AS max_id FROM S) s \
			JOIN (SELECT MAX(PID) AS max_id FROM P) p \
			JOIN (SELECT MAX(SPID) AS max_id FROM SP) sp"

	@@query_suppliers = "SELECT * FROM S \
			WHERE SID BETWEEN %i AND %i AND \
			SName IS NOT NULL AND \
			SCity IS NOT NULL AND \
			Address IS NOT NULL"

	@@query_parts = "SELECT PName, HTP, CAST(Weight AS DECIMAL(8,4)) AS Weight FROM P \
			WHERE PID BETWEEN %i AND %i AND
			PName IS NOT NULL AND \
			Weight > 0"

	@@query_shipments = "SELECT \
			S.SName AS Supplier, \
			S.Address AS Address, \
			S.SCity AS City, \
			P.PName AS Part, \
			CAST(P.Weight AS DECIMAL(8,4)) AS PartWeight, \
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
			SP.SPID BETWEEN %i AND %i AND \
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
		HAVING SPWeight BETWEEN 0 AND 1500"

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

		@storage.query("UPDATE suppliers  \
				INNER JOIN ( \
					SELECT sid, COUNT( * ) AS value \
					FROM shipments \
					GROUP BY sid \
					)shipments_all ON shipments_all.sid = suppliers.id \
				LEFT JOIN ( \
					SELECT sid, COUNT( * ) AS value \
					FROM shipments \
					WHERE DATEDIFF( ship_date, order_date ) > period \
					GROUP BY sid \
					)shipments_delayed ON shipments_delayed.sid = suppliers.id \
				SET suppliers.risk = CASE  \
					WHEN shipments_delayed.value / shipments_all.value >2 /3 THEN 3  \
					WHEN shipments_delayed.value / shipments_all.value >1 /3 THEN 2  \
					ELSE 1 END" ")

		puts "Values for #{@affiliate_id} affiliate added to warehouse: suppliers (#{@last_s}, #{@max_s}), parts (#{@last_p}, #{@max_p}), relations (#{@last_sp}, #{@max_sp})"
	end

	protected
	def add_supplier(supplier)
		@suppliers << {:name => supplier["name"], :city => supplier["city"], :address => supplier["address"], :orders => supplier["orders"], :delays => ["delays"]}
	end

	protected
	def add_part(part)
		@parts << {:name => part["PName"], :htp => part["HTP"], :weight => part["Weight"]}
	end

	protected
	def add_shipment(shipment)
		@shipments << {:supplier => shipment["Supplier"], :city => shipment["City"], :address => shipment["Address"], :supplier_id => 0,
			:part => shipment["Part"], :part_weight => shipment["PartWeight"], :qty => shipment["Quantity"], :part_id => 0,
			:order_date => shipment["OrderDate"], :ship_date => shipment["ShipDate"], :period => shipment["Period"], 
			:weight => shipment["SPWeight"], :price => shipment["Price"]}
	end

	protected
	def create_suppliers()
		return if @suppliers.empty?

		suppliers_values = String.new
		@suppliers.each do |supplier|
			suppliers_values << ',' if not suppliers_values.empty?
			suppliers_values << '("' << supplier[:name] << '","' << supplier[:address] << '","' << supplier[:city] << '")'
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
						<< shipment[:qty] << ',' << shipment[:price] << ',' << shipment[:weight] << ',"' \
						<< shipment[:order_date] << '",' << shipment[:period] << ',"' << shipment[:ship_date] << '")'
		end

		return if shipments_values.empty?
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

	protected
	def extract_max_ids()
		begin
			res = @con.query(@@query_max_ids).fetch_hash

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
			@suppliers = Array.new
			@con.query(@@query_suppliers % [@last_s + 1, @max_s]).each_hash do |row|
				self.add_supplier row
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
			@parts = Array.new
			@con.query(@@query_parts % [@last_p + 1, @max_p]).each_hash do |row|
				self.add_part row
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
			@shipments = Array.new
			@con.query(@@query_shipments % [@last_sp + 1, @max_sp]).each_hash do |row|
				self.add_shipment row
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
		begin
			@con = SQLite3::Database.new "company.db"
			@con.results_as_hash = true
		rescue SQLite3::Exception => e
			puts e
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
			@con.execute(@@query_max_ids) do |row|
				@max_s = row['SID'] unless row['SID'] == nil
				@max_p = row['PID'] unless row['PID'] == nil
				@max_sp = row['SPID'] unless row['SPID'] == nil
			end
		rescue SQLite3::Exception => e
			puts e
			self.close_connection
		end
	end

	protected
	def extract_suppliers()
		begin
			@suppliers = Array.new
			@con.execute(@@query_suppliers % [@last_s + 1, @max_s]) do |row|
				self.add_supplier
			end
		rescue SQLite3::Exception => e
			puts e
			self.close_connection
		end
	end

	protected
	def extract_parts()
		begin
			@parts = Array.new
			@con.execute(@@query_parts % [@last_p + 1, @max_p]) do |row|
				self.add_part row
			end
		rescue SQLite3::Exception => e
			puts e
			self.close_connection
		end
	end
	
	protected
	def extract_shipments()
		begin
			@shipments = Array.new
			@con.execute(@@query_shipments % [@last_sp + 1, @max_sp]) do |row|
				self.add_shipment row
			end
		rescue SQLite3::Exception => e 	
   			puts e
   			self.close_connection
   		end
	end
end

begin
	puts 'Start ETL process'

	databases = [
		AffiliateOne,
		#AffiliateTwo
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