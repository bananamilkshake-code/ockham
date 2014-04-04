#ifndef OLAP_H
#define OLAP_H

#include <map>
#include <string>
#include <mysql/mysql.h>

class OLAP
{
	enum Types : uint8_t
	{
		TIME,
		PLACE,
		DETAIL
	};

	MYSQL *connection;

public:
	typedef std::map<std::string, std::map<std::string, std::map<std::string, float>>> cube_t;

	OLAP();
	~OLAP();

	cube_t calculate(uint8_t dim_1, uint8_t dim_2, uint8_t dim_3);

private:
	cube_t convert_result(MYSQL_RES *result);
};

#endif // OLAP_H
