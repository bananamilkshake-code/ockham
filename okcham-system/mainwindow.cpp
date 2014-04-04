#include "mainwindow.h"
#include "ui_mainwindow.h"

#include <array>
#include <regex>
#include <sstream>
#include <unordered_set>

#include <QMessageBox>

#include "etl.h"

const QStringList MainWindow::DIMENSIONS =
{
	"Time",
	"Place",
	"Detail"
};

MainWindow::MainWindow(QWidget *parent) :
	QMainWindow(parent),
	ui(new Ui::MainWindow),
	olap(OLAP())
{
	ui->setupUi(this);

	this->set_olap_dimensions();
}

MainWindow::~MainWindow()
{
	delete ui;
}

void MainWindow::on_button_set_etl_cron_clicked()
{
	this->update_etl_schedule(this->get_cron_parameters());
}

void MainWindow::on_button_cron_notation_clicked()
{
	this->update_etl_schedule(this->get_cron_statement());
}

void MainWindow::on_button_run_ETL_clicked()
{
	this->perform_etl();
}

void MainWindow::set_olap_dimensions()
{
	this->ui->combo_x->addItems(DIMENSIONS);
	this->ui->combo_y->addItems(DIMENSIONS);

	this->ui->combo_x->setCurrentIndex(0);
	this->ui->combo_y->setCurrentIndex(1);

	this->update_olap_combos('x', 0);
}

void MainWindow::perform_etl()
{
	this->ui->text_etl_process->insertPlainText("Start ETL process\n");

	auto success = ETL::run();
	if (!success)
		this->ui->text_etl_process->insertPlainText("Error on ETL performance: check script path to \"etl.rb\"\n");
	else
		this->ui->text_etl_process->insertPlainText("End ETL process\n");

	this->olap.fill_values();
}

void MainWindow::update_olap_combos(uint8_t combo_semantic, int index)
{
	auto other_combo = (combo_semantic == 'x' ? this->ui->combo_y : this->ui->combo_x);

	if (other_combo->currentIndex() == index)
		other_combo->setCurrentIndex((index + 1) % DIMENSIONS.size());

	auto dim_x = this->ui->combo_x->currentIndex();
	auto dim_y = this->ui->combo_y->currentIndex();

	if (dim_x == -1 || dim_y == -1)
		return;

	uint8_t dim_z = this->get_z_dimension();

	this->ui->label_z->setText(DIMENSIONS.at(dim_z));

	this->ui->combo_detalisation_1->clear();
	this->ui->combo_detalisation_2->clear();
	this->ui->combo_detalisation_3->clear();

	this->ui->combo_detalisation_1->addItems(OLAP::DETALIZATION[dim_x]);
	this->ui->combo_detalisation_2->addItems(OLAP::DETALIZATION[dim_y]);
	this->ui->combo_detalisation_3->addItems(OLAP::DETALIZATION[dim_z]);

	this->fill_z_values();
}

static constexpr int8_t EVERY = -1;
static constexpr char *EVERY_CHAR = "*";

std::string MainWindow::get_cron_parameters() const
{
	std::ostringstream stream_parameters;

	const int time_periods[] =
	{
		this->ui->spin_minute->value(),
		this->ui->spin_hour->value(),
		this->ui->spin_month_day->value(),
		this->ui->spin_month->value(),
		this->ui->spin_week_day->value()
	};

	for (auto value : time_periods)
	{
		if (stream_parameters.tellp())
			stream_parameters << " ";
		(value == EVERY) ? stream_parameters << EVERY_CHAR : stream_parameters << (int32_t)value;
	}

	return stream_parameters.str();
}

std::string MainWindow::get_cron_statement() const
{
	return this->ui->line_cron->text().toUtf8().constData();
}

uint8_t MainWindow::get_z_dimension() const
{
	uint8_t dim_x = this->ui->combo_x->currentIndex();
	uint8_t dim_y = this->ui->combo_y->currentIndex();

	uint8_t dim_z = 0;
	while (dim_z == dim_x || dim_z == dim_y)
		dim_z++;

	return dim_z;
}

void MainWindow::update_etl_schedule(std::string options) const
{
	std::regex cron_statement_regex("");

	if (!std::regex_search(options, cron_statement_regex, std::regex_constants::match_continuous))
	{
		QMessageBox error_statement;
		error_statement.setText("Wrong error statement for cron \"" + QString(options.c_str()) + "\"");
		error_statement.exec();
		return;
	}

	ETL::set_cron(options);
}

void MainWindow::on_combo_x_currentIndexChanged(int index)
{
	this->update_olap_combos('x', index);
}

void MainWindow::on_combo_y_currentIndexChanged(int index)
{
	this->update_olap_combos('y', index);
}

void MainWindow::on_button_olap_clicked()
{
	uint8_t dim_1 = this->ui->combo_x->currentIndex();
	uint8_t detalisation_1 = this->ui->combo_detalisation_1->currentIndex();

	uint8_t dim_2 = this->ui->combo_y->currentIndex();
	uint8_t detalisation_2 = this->ui->combo_detalisation_2->currentIndex();

	uint8_t dim_3 = DIMENSIONS.indexOf(this->ui->label_z->text());
	uint8_t detalisation_3 = this->ui->combo_detalisation_3->currentIndex();

	auto cube = this->olap.calculate(OLAP::Type(dim_1), detalisation_1, OLAP::Type(dim_2), detalisation_2, OLAP::Type(dim_3), detalisation_3, this->ui->combo_z_value->currentText().toUtf8().constData());
	this->fill_olap_cube(cube);
}

void MainWindow::on_button_classify_clicked()
{
}

void MainWindow::on_button_clasterize_clicked()
{
}

void MainWindow::fill_olap_cube(OLAP::cube_t cube)
{
	auto table = this->ui->table_olap;
	table->setColumnCount(1);
	table->setRowCount(1);

	std::unordered_set<std::string> values_list;

	auto row_id = 0;
	auto col_id = 0;

	for (auto col : cube)
	{
		auto row = col.second;

		auto col_header = col.first;
		if (col_header !=  "NULL")
		{
			table->setColumnCount(col_id + 1);
			QTableWidgetItem* header_item = new QTableWidgetItem(col_header.c_str(),QTableWidgetItem::Type);
			table->setHorizontalHeaderItem(col_id++, header_item);
		}

		for (auto record : row)
		{
			auto row_header = record.first;
			if (row_header == "NULL")
				continue;

			if (!values_list.insert(row_header).second)
				continue;

			table->setRowCount(row_id + 1);
			QTableWidgetItem* header_item = new QTableWidgetItem(row_header.c_str(),QTableWidgetItem::Type);
			table->setVerticalHeaderItem(row_id++, header_item);
		}
	}
	values_list.insert("NULL");
}

void MainWindow::fill_z_values()
{
	uint8_t dimension = this->get_z_dimension();
	uint8_t detalisation = this->ui->combo_detalisation_3->currentIndex();

	if (dimension >= DIMENSIONS.size() || detalisation > OLAP::DETALIZATION[dimension].size())
		return;

	this->ui->combo_z_value->clear();
	this->ui->combo_z_value->addItems(this->olap.get_values_list(OLAP::Type(dimension), detalisation));
}

void MainWindow::on_combo_detalisation_3_currentIndexChanged(int index)
{
	this->fill_z_values();
}
