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

	static const QStringList DETALIZATION[];
	static const std::vector<std::vector<std::string>> ROW_NAMES;

	typedef std::map<std::string, std::map<std::string, std::map<std::string, float>>> cube_t;
	typedef std::vector<std::string> values_list_t;

	OLAP();
	~OLAP();

	cube_t calculate(Type dim_1, uint8_t detalization_1, Type dim_2, uint8_t detalization_2, Type dim_3, uint8_t detalization_3);

	values_list_t get_years() const;
	values_list_t get_monthes() const;
	values_list_t get_suppliers() const;
	values_list_t get_cities() const;
	values_list_t get_details() const;
	values_list_t get_htp() const;

private:
	cube_t convert_result(MYSQL_RES *result);
};

#endif // OLAP_H
