CREATE TABLE tblTypeDim (
	Type_Key TINYINT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	Type VARCHAR(3) NOT NULL)
GO

CREATE TABLE tblBreedDim (
	Breed_Key TINYINT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	Breed VARCHAR(255) NOT NULL)
GO

CREATE TABLE tblGenderDim (
	Gender_Key TINYINT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	Gender VARCHAR(6) NOT NULL)
GO

CREATE TABLE tblRescuerDim (
	Rescuer_Key INT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	RescuerID INT NOT NULL)
GO

CREATE TABLE tblColorDim (
	Color_Key TINYINT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	Color VARCHAR(255) NOT NULL)
GO

CREATE TABLE tblSizeDim (
	Size_Key TINYINT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	Size VARCHAR(13) NOT NULL)
GO

CREATE TABLE tblMedicalConditionDim (
	MC_Key TINYINT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	MedicalCondition VARCHAR(8) NOT NULL)
GO

CREATE TABLE tblHealthConditionDim (
	HealthCondition_Key TINYINT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	HealthCondition VARCHAR(14) NOT NULL)
GO

CREATE TABLE tblStateDim (
	State_Key TINYINT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	State VARCHAR(255) NOT NULL)
GO

CREATE TABLE tblPetFact (
	Pet_Key INT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	Pet_ID INT NOT NULL,
	Age INT NOT NULL,
	Breed1_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblBreedDim(Breed_Key),
	Breed2_Key TINYINT FOREIGN KEY REFERENCES tblBreedDim(Breed_Key),
	Gender_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblGenderDim(Gender_Key),
	Color1_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblColorDim(Color_Key),
	Color2_Key TINYINT FOREIGN KEY REFERENCES tblColorDim(Color_Key),
	Color3_Key TINYINT FOREIGN KEY REFERENCES tblColorDim(Color_Key),
	Maturity_Size_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblSizeDim(Size_Key),
	FurLength_Size_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblSizeDim(Size_Key),
	Vaccinated_MC_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblMedicalConditionDim(MC_Key),
	Dewormed_MC_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblMedicalConditionDim(MC_Key),
	Sterilized_MC_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblMedicalConditionDim(MC_Key),
	HealthCondition_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblMedicalConditionDim(MC_Key),
	Quantity INT NOT NULL,
	Fee INT,
	State_Key TINYINT NOT NULL FOREIGN KEY REFERENCES tblStateDim(State_Key),
	Rescuer_Key INT NOT NULL FOREIGN KEY REFERENCES tblRescuerDim(Rescuer_Key),
	IsCurrent char(5)
	)
GO

--1.Có bao nhiêu con chó bị chấn thương (nhẹ và nghiêm trọng) trên 1 tuổi
SELECT COUNT(*) AS Total
FROM tblPetFact AS P
JOIN tblHealthConditionDim HC ON P.HealthCondition_Key = HC.HealthCondition_Key
WHERE (HC.HealthCondition = 'Minor Injury' OR HC.HealthCondition = 'Serious Injury') AND Age > 1
GO

--2. Có bao nhiêu con mèo tam thể (có 3 màu) tại từng bang
SELECT S.State, COUNT(*) AS Total
FROM tblPetFact AS P
JOIN tblStateDim AS S ON P.State_Key = S.State_Key
WHERE Color2_Key IS NOT NULL AND Color3_Key IS NOT NULL
GROUP BY S.State
GO

--3. Chi phí nhận nuôi trung bình của vật nuôi giống lai đã được tiêm phòng 
SELECT AVG(Fee) AS AverageFee
FROM tblPetFact AS P
WHERE Breed2_Key IS NOT NULL AND Vaccinated = 'Yes'
GO

SELECT * FROM tblPetFact