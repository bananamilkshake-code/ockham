#ifndef OLAP_H
#define OLAP_H

#include <map>
#include <string>
#include <vector>
#include <mysql/mysql.h>

#include <QStringList>

class OLAP
{
	MYSQL *connection;

public:
	enum Type : uint8_t
	{
		TIME,
		PLACE,
		DETAIL
	};

	typedef std::map<std::string, std::map<std::string, std::string>> cube_t;

	static const QStringList DETALIZATION[];
	static const std::vector<std::vector<std::string>> ROW_NAMES;

	static const std::string ALL;

	OLAP();
	~OLAP();

	cube_t calculate(Type dim_1, uint8_t detalization_1, Type dim_2, uint8_t detalization_2, Type dim_3, uint8_t detalization_3, std::string dim_z_value);

	void fill_values();

	const QStringList& get_values_list(Type dimension, uint8_t detalisation);

private:
	typedef std::vector<std::vector<QStringList>> values_lists_t;
	values_lists_t lists;

	cube_t convert_result(MYSQL_RES *result);
};

#endif // OLAP_H
