#!/usr/bin/python
# -*- coding: UTF-8 -*-

import argparse
import calendar
import datetime
import random
import string
import sys
import time

def genParser():
	parser = argparse.ArgumentParser()

	# CVS file names
	parser.add_argument ('-sname', '--supplier-name')
	parser.add_argument ('-pname', '--part-name')
	parser.add_argument ('-spname', '--relation-name')

	# Counts of records
	parser.add_argument ('-sqty', '--supplier-quantity', default=100, type=int)
	parser.add_argument ('-pqty', '--part-quantity', default=100, type=int)
	parser.add_argument ('-spqty', '--relation-quantity', default=100, type=int)

	# Percent of errors in records
	parser.add_argument ('-serr', '--supplier-errors', default=10, type=int)
	parser.add_argument ('-perr', '--part-errors', default=10, type=int)
	parser.add_argument ('-sperr', '--relation-errors', default=10, type=int)

	return parser

def is_error(percent):
	e = random.randrange(0, 100)
	error = e < percent
	if error:
		print "Aha"
	return error

# Make an error in value
def error_line(line):
	"""
	MISS_CHAR = 0
	WRONG_CHAR = 1
	MISSED_VALUE = 2
	error = random.choice([MISS_CHAR, WRONG_CHAR, MISSED_VALUE])

	hit = random.randint(0, len(line) - 1)

	return {
		MISS_CHAR: line[:hit] + line[(hit+1):],
		WRONG_CHAR: line[:hit] + random.choice((string.letters + string.digits).replace(line[(hit):], '')),
		MISSED_VALUE: "NULL"
	}.get(error, line)
	"""
	return "NULL"

def error_number(number):
	TOO_BIG = 0
	TOO_SMALL = 1
	SIGNED = 2
	MISSED_VALUE = 3
	error = random.choice([TOO_BIG, TOO_SMALL, SIGNED, MISSED_VALUE])

	return {
		TOO_BIG: number * 100,
		TOO_SMALL: number // 100,
		SIGNED: number * (-1),
		MISSED_VALUE: "NULL"
	}.get(error)

def error_dates(date1, date2):
	MIXED = 0
	MISSED = 1
	error = random.choice([MIXED, MISSED])

	return {
		MIXED: (date2, date1),
		MISSED: [("NULL", date2), (date1, "NULL")][random.getrandbits(1)]
	}.get(error)

# Value generation
def gen_date(begin=0, delta=0):
	if begin == 0:
		now = datetime.datetime.now()

		year = random.choice(range(2011, now.year))
		month = random.choice(range(1, 12))
		day = random.choice(range(1, calendar.monthrange(year, month)[1]))

		date = datetime.datetime(year, month, day).date()
	else:
		date = (datetime.datetime(begin.year, begin.month, begin.day) + datetime.timedelta(days=delta + random.randrange(-10, +10))).date()
	return date


def normal_distribution(x):
	return x

def weight_distribution(x):
	return 1.0 / x;

def gen_int(range_l, range_r, fun=normal_distribution):
	return fun(random.randrange(range_l, range_r + 1))

def gen_float(range_l, range_r, fun=normal_distribution):
	return fun(random.uniform(range_l, range_r))

# Supplier generation
def gen_supplier_name():
	supplier_names = [
		"Coca-cola",
		"Adobe",
		"BMV",
		"Canon",
		"Lada",
		"Casio",
		"Cisco",
		"Hitachi",
		"IBM",
		"kawasaki",
		"Kodak",
		"Lotus",
		"Motorola",
		"NEC",
		"Nike",
		"Nikon",
		"Nintendo",
		"Nokia",
		"Novell",
		"Oracle",
		"Sanyo",
		"Siemens",
		"Samsung",
		"TDK",
		"Philips"
	]

	return random.choice(supplier_names)

def gen_city():
	cities = [
		"Челябинск",
		"Москва",
		"Лондон",
		"Токио",
		"Париж",
		"Мадрид",
		"Нью-Йорк",
		"Вегас",
		"Амстердам",
		"Берлин",
		"Пекин",
		"Торонто",
		"Рим",
		"Вена",
		"Цюрих"
	]

	return random.choice(cities)

def gen_address():
	streets = [
		"Victory St.",
		"Kitano St.",
		"Yellow Blvd.",
		"Red Blvd.",
		"Summer Ave."
		"Sunny Ave.",
		"Leaf St.",
		"Old St.",
		"Long Blv.",
		"Short Blv.",
		"Noisy St.",
		"Greeting Ave."
	]

	return random.choice(streets) + ", " + str(gen_int(1, 100)) + "-" + str(gen_int(1, 100))

# Part generation
def gen_part_name():
	part_names = [
		"Педаль",
		"Болтик",
		"Винтик",
		"Гайка",
		"Гвоздь",
		"Двигатель",
		"Материнская плата",
		"Транзистор",
		"Резистор",
		"Коллайдер",
		"Гидроусилитель",
		"Сублиматор",
		"Телепорт",
		"Измельчитель"
	]

	return random.choice(part_names)

def generate_suppliers(file_name, number, error):
	supplier_file = open(file_name, "w")
	for i in range(number):
		name = gen_supplier_name()
		city = gen_city()
		address = gen_address()
		risk = gen_int(1, 3)
		if is_error(error):
			#print name, city, address, risk
			NAME_ERROR = 1
			CITY_ERROR = 2
			ADDRESS_ERROR = 3
			RISK_ERROR = 4
			error_field = random.choice([NAME_ERROR, CITY_ERROR, ADDRESS_ERROR, RISK_ERROR])
			if error_field == NAME_ERROR:
				name = error_line(name)
			elif error_field == CITY_ERROR:
				city = error_line(city)
			elif error_field == ADDRESS_ERROR:
				address = error_line(address)
			elif error_field == RISK_ERROR:
				risk = error_number(risk)
			#print error_field
			#print name, city, address, risk
		supplier_file.write("%s,\"%s\",\"%s\",\"%s\",%s\n" % (i, name, city, address, risk))
	supplier_file.close()

def generate_parts(file_name, number, error):
	part_file = open(file_name, "w")
	for i in range(number):
		name = gen_part_name()
		htp = gen_int(0, 1)
		weight = gen_float(0.1, 1500, weight_distribution)
		if is_error(error):
			#print name, htp, weight
			NAME_ERROR = 1
			HTP_ERROR = 2
			WEIGHT_ERROR = 3
			error_field = random.choice([NAME_ERROR, HTP_ERROR, WEIGHT_ERROR])
			if error_field == NAME_ERROR:
				name = error_line(name)
			elif error_field == HTP_ERROR:
				htp = error_number(htp)
			elif error_field == WEIGHT_ERROR:
				weight = error_number(weight)
			#print error_field
			#print name, htp, weight
		part_file.write("%s,\"%s\",%s,%s\n" % (i, name, htp, weight))
	part_file.close()

def generate_shipments(file_name, number, error, suppliers_qty, parts_qty):
	relation_file = open(args.relation_name, "w")
	error = args.relation_errors
	for i in range(number):
		sid = random.randrange(1, suppliers_qty)
		pid = random.randrange(1, parts_qty)
		qty = gen_int(1, 100)
		price = gen_float(0.1, 1000)
		order_date = gen_date()
		period = gen_int(0, 60)
		ship_date = gen_date(order_date, period)
		if is_error(error):
			#print sid, pid, qty, price, order_date, period, ship_date 
			QUANTITY_ERROR = 1
			PRICE_ERROR = 2
			DATE_ERROR = 3
			PERIOD_ERROR = 4
			error_field = random.choice([QUANTITY_ERROR, PRICE_ERROR, DATE_ERROR, PERIOD_ERROR])
			if error_field == QUANTITY_ERROR:
				qty = error_number(qty)
			elif error_field == PRICE_ERROR:
				price = error_number(price)
			elif error_field == DATE_ERROR:
				order_date, ship_date = error_dates(order_date, ship_date)
			elif error_field == PERIOD_ERROR:
				period = error_number(period)
			#print error_field
			#print sid, pid, qty, price, order_date, period, ship_date
		relation_file.write("%s,%s,%s,%s,%s,\"%s\",%s,\"%s\"\n" % (i + 1, sid, pid, qty, price, order_date, period, ship_date))
	relation_file.close()

if __name__ == '__main__':
	random.seed()

	parser = genParser()
	args = parser.parse_args(sys.argv[1:])

	generate_suppliers(args.supplier_name, args.supplier_quantity, args.supplier_errors)
	generate_parts(args.part_name, args.part_quantity, args.part_errors)
	generate_shipments(args.relation_name, args.relation_quantity, args.relation_errors, args.supplier_quantity, args.part_quantity)