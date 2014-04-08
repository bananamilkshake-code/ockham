#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>

#include "olap.h"

class QTableWidget;

namespace Ui {
class MainWindow;
}

class MainWindow : public QMainWindow
{
	Q_OBJECT

	static const QStringList DIMENSIONS;

public:
	explicit MainWindow(QWidget *parent = 0);
	~MainWindow();

private slots:
	void on_button_set_etl_cron_clicked();
	void on_button_cron_notation_clicked();
	void on_button_run_ETL_clicked();

	void on_combo_x_currentIndexChanged(int index);
	void on_combo_y_currentIndexChanged(int index);
	void on_button_olap_clicked();

	void on_button_classify_clicked();
	void on_button_clasterize_clicked();

	void on_combo_detalisation_3_currentIndexChanged(int index);

	void on_button_templates_clicked();

private:
	Ui::MainWindow *ui;
	OLAP olap;

	void add_col(QTableWidget *table, std::string header);
	void add_row(QTableWidget *table, std::string header);
	void set_cell(QTableWidget *table, size_t row, size_t col, std::string value);
	void clear_table(QTableWidget *table);

	void set_olap_dimensions();

	void perform_etl();
	void update_olap_combos(uint8_t combo_semantic, int index);
	void fill_olap_cube(OLAP::cube_t cube);
	void fill_z_values();

	void perform_classification();
	void find_templates();

	std::string get_cron_parameters() const;
	std::string get_cron_statement() const;

	uint8_t get_z_dimension() const;

	void update_etl_schedule(std::string options) const;
};

#endif // MAINWINDOW_H
