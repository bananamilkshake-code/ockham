#!/usr/bin/python
# -*- coding: UTF-8 -*-

import argparse
import calendar
import datetime
import sys
import random
import string
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
	return random.random < percent / 100.0

# Make an error in value
def error_line(line):
	error_type = ["miss_char", "wrong_char", "missed_value"]
	error = random.choice(error_type)

	hit = random.randint(0, len(line) - 1)

	return {
		"miss_char": line[:hit] + line[(hit+1):],
		"wrong_char": line[:hit] + random.choice((string.letters + string.digits).replace(line[(hit):], '')),
		"missed_value": None
	}.get(error, line)

def error_int(number):
	error_type = ["too_big", "too_small", "signed", "missed_value"]
	error = random.choice(error_type)

	return {
		"too_big": number * 100,
		"too_small": number // 100,
		"signed": number * (-1),
		"missed_value": None
	}.get(error, number)

# Value generation
def gen_unixtimestamp(begin=0, delta=0, error=0):
	if begin == 0:
		now = datetime.datetime.now()

		year = random.choice(range(1960, now.year))
		month = random.choice(range(1, 12))
		day = random.choice(range(1, 28))

		date = datetime.datetime(year, month, day)
	else:
		start = datetime.datetime.fromtimestamp(begin)

		date = datetime.datetime(start.year, start.month, start.day) + datetime.timedelta(days=delta + random.randrange(0, 30))

	return calendar.timegm(date.utctimetuple())

def gen_int(range_l, range_r, error=0):
	result = random.randrange(range_l, range_r+1)
	if is_error(error):
		error_int(result)
	return result

def gen_float(range_l, range_r, decimal, error=0):
	result = decimal * random.uniform(range_l, range_r)
	if is_error(error):
		error_int(result)
	return result

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

def gen_city(error):
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

	city = random.choice(cities)
	if (is_error(error)):
		error_line(city)
	return city

def gen_address(error):
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

	address = random.choice(streets) + ", " + str(gen_int(1, 100)) + "-" + str(gen_int(1, 100))
	if (is_error(error)):
		error_line(address)
	return address

# Part generation
def gen_part_name():
	part_names = [
		"Педаль",
		"Болтик",
		"Винтик",
		"Гака",
		"Гвоздь",
		"Двигатель",
		"Материнсская плата",
		"Транзистор",
		"Резистор",
		"Коллайдер",
		"Гидроусилитель",
		"Сублиматор",
		"Телепорт",
		"Измельчитель"
	]

	return random.choice(part_names)

if __name__ == '__main__':
	random.seed()

	parser = genParser()
	args = parser.parse_args(sys.argv[1:])

	supplier_file = open(args.supplier_name, "w")
	error = args.supplier_errors / 4		# divide be number of cols where error can occure
	for i in range(args.supplier_quantity):
		name = gen_supplier_name()
		city = gen_city(error)
		address = gen_address(error)
		risk = gen_int(1, 3, error)
		supplier_file.write("%i,\"%s\",\"%s\",\"%s\",%i\n" % (i, name, city, address, risk))
	supplier_file.close()

	part_file = open(args.part_name, "w")
	error = args.part_errors				# divide be number of cols where error can occure
	for i in range(args.part_quantity):
		name = gen_part_name()
		htp = gen_int(0, 1)
		weight = gen_float(0.1, 1.5, 100, error)
		part_file.write("%i,\"%s\",%i,%f\n" % (i, name, htp, weight))
	part_file.close()

	relation_file = open(args.relation_name, "w")
	error = args.relation_errors / 5		# divide be number of cols where error can occure
	for i in range(args.relation_quantity):
		sid = random.randrange(1, args.supplier_quantity)
		pid = random.randrange(1, args.part_quantity)
		qty = gen_int(1, 100, error)
		price = gen_float(0.1, 10000, 100, error)
		order_date = gen_unixtimestamp()
		period = gen_int(10, 60)
		ship_date = gen_unixtimestamp(order_date, period, error)
		period = period + gen_int(0, period, error)
		relation_file.write("%i,%i,%i,%i,%f,\"%s\",%i,%s\n" % (i, sid, pid, qty, price, order_date, period, ship_date))
	relation_file.close()