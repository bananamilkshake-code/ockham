#-------------------------------------------------
#
# Project created by QtCreator 2014-03-27T20:27:41
#
#-------------------------------------------------

QT       += core gui

greaterThan(QT_MAJOR_VERSION, 4): QT += widgets

TARGET = okcham-system
TEMPLATE = app


SOURCES += main.cpp\
	mainwindow.cpp \
    etl.cpp \
    olap.cpp

HEADERS  += mainwindow.h \
    etl.h \
    olap.h \
    mysql_connection_details.h

FORMS    += mainwindow.ui

QMAKE_CXXFLAGS += -std=c++0x

unix|win32: LIBS += -lmysqlclient
