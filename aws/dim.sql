CREATE TABLE EV_registration (
  VIN varchar(10) PRIMARY KEY,
  Model_ID int NOT NULL,
  County_ID int NOT NULL,
  City_ID int NOT NULL,
  State_ID int NOT NULL,
  Postal_Code_ID int NOT NULL,
  Electric_Utility_ID int,
  Legislative_District_ID int,
  Census_Tract_ID int,
  Sales_Count int,  -- Optional, depending on your data source
  FOREIGN KEY (Model_ID) REFERENCES Dim_Model(Model_ID),
  FOREIGN KEY (County_ID) REFERENCES Dim_County(County_ID),
  FOREIGN KEY (City_ID) REFERENCES Dim_City(City_ID),
  FOREIGN KEY (State_ID) REFERENCES Dim_State(State_ID),
  FOREIGN KEY (Postal_Code_ID) REFERENCES Dim_Postal_Code(Postal_Code_ID),
  FOREIGN KEY (Electric_Utility_ID) REFERENCES Dim_Electric_Utility(Electric_Utility_ID),
  FOREIGN KEY (Legislative_District_ID) REFERENCES Dim_Legislative_District(Legislative_District_ID),
  FOREIGN KEY (Census_Tract_ID) REFERENCES Dim_2020_Census_Tract(Census_Tract_ID)
);
CREATE TABLE Dim_Model (
  Model_ID int PRIMARY KEY AUTO_INCREMENT,
  Make varchar(255) NOT NULL,
  Model varchar(255) NOT NULL,
  Model_Year int NOT NULL,
  Electric_Vehicle_Type varchar(255) NOT NULL,
  Base_MSRP decimal(10,2) NOT NULL
);
CREATE TABLE Dim_County (
  County_ID int PRIMARY KEY AUTO_INCREMENT,
  County_Name varchar(255) NOT NULL,
  State_ID int NOT NULL,
  FOREIGN KEY (State_ID) REFERENCES Dim_State(State_ID)
);
CREATE TABLE Dim_City (
  City_ID int PRIMARY KEY AUTO_INCREMENT,
  City_Name varchar(255) NOT NULL,
  County_ID int NOT NULL,
  State_ID int NOT NULL,
  FOREIGN KEY (County_ID) REFERENCES Dim_County(County_ID),
  FOREIGN KEY (State_ID) REFERENCES Dim_State(State_ID)
);

CREATE TABLE Dim_State (
  State_ID int PRIMARY KEY AUTO_INCREMENT,
  State_Abbreviation varchar(2) NOT NULL,
  State_Name varchar(255) NOT NULL
);
CREATE TABLE Dim_Electric_Utility (
  Electric_Utility_ID int PRIMARY KEY AUTO_INCREMENT,
  Electric_Utility_Name varchar(255) NOT NULL
);
CREATE TABLE Dim_Legislative_District (
  Legislative_District_ID int PRIMARY KEY AUTO_INCREMENT,
  District_Number varchar(255) NOT NULL,
  State_ID int NOT NULL,
  FOREIGN KEY (State_ID) REFERENCES Dim_State(State_ID)
);
CREATE TABLE Dim_2020_Census_Tract (
  Census_Tract_ID int PRIMARY KEY AUTO_INCREMENT,
  Census_Tract varchar(255) NOT NULL,
  County_ID int NOT NULL,
  State_ID int NOT NULL,
  FOREIGN KEY (County_ID) REFERENCES Dim_County(County_ID),
  FOREIGN KEY (State_ID) REFERENCES Dim_State(State_ID)
);
