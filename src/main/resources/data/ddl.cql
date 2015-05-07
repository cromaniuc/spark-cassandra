CREATE KEYSPACE ElectricityBills WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};

use ElectricityBills;

CREATE TABLE ElectricityBill (
	id int PRIMARY KEY,
	name varchar,
	billingPeriodFrom varchar,
	billingPeriodTo varchar,
	lastBill int,
	currentBill int
);
