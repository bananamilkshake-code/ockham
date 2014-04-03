#ifndef ETL_H
#define ETL_H

#include <string>

class ETL
{
	static const std::string SCRIPT_PATH;

public:
	static bool run();
	static void set_cron(std::string options);
};

#endif // ETL_H
