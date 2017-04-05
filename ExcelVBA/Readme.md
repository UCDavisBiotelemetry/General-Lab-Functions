Installation Directions
----
These excel macro functions can be saved into the "Personal" workbook (Personal.xlam) (default: hidden). To create, modify, and view VBA (visual basic for applications) code you'll need to enable the "Developer" ribbon or menu in Excel, then click on "Visual Basic".


Navigate into the appropriately named sub-item (without the .vba extension) in the Project tree by double-clicking on said sub-item.  If the sub-item does not exist, right click in the Project tree and select "Insert" and then e.g. "Module". Then copy-paste the contents of the .vba files into the main MS VBA window (top-center or top-right), click on "Debug"->"Compile VBAProject" in the main menu.

Function Definitions
----
ThisWorkbook
====
The chain of functions and procedures (Workbook_Open -> App_WorkbookOpen -> TemplateCSVs -> BuildDTString) should, when the opened file is a CSV or TSV, auto-format datetime strings to include seconds and then autofit all columns. You should not need to manually call any of these functions.


Module1
====
CSVifyArray(inRng As Range, Optional sep As String = ",", Optional ColFirst As Boolean = False)
	Description: This function will allow the creation of a CSV string from a standard rectangular range of cells, placing commas between each item. Strings are not quoted.
	Returns: a string into a single cell
	Use example
		Formula: =CSVifyArray(A1:B3,";",True)
           Output: a semicolon seperated list where the list is created as the values from **[A1];[A2];[A3];[B1];[B2];[B3]**

CSVifyArStrings(inRng As Range, Optional quotechar As String = "'", Optional sep As String = ",", Optional ColFirst As Boolean = False)
	Description: Quote all values in the CSV string (otherwise as above), regardless of data type.
	Returns: a string into a single cell
	Use example
		Formula: =CSVifyArray(A1:B3,""","-",True)
           Output: a dash seperated list where the list is created as the values from **"[A1]"-"[A2]"-"[A3]"-"[B1]"-"[B2]"-"[B3]"**

Function MakePair(inRng As Range, ParamArray strs() As Variant)
	Description: makes a set of values, quoted as desired, from a single row
     Returns: a string into a singel cell
     Use example
		Formula: =MakePair(A1:C1,[True,False,True])
		Output: **'[A1]',[B1],'[C1]'**

LastRow(pArray As Variant)
	Description: returns the last row number of a selected range to contain any data relative to the start of that range.
     Returns: an integer into a single cell (or as a return value for another function)
     Use example
		Formula: =LastRow(C4:F9)
           Output: an integer from 1 to 6, depending on what the last row is that contains any value, formula (or formatting?)

LastCol(pArray As Variant)
	Description: returns the last column number of a selected range to contain any data relative to the start of that range.
     Returns: an integer into a single cell (or as a return value for another function)
     Use example
		Formula: =LastCol(C4:F9)
           Output: an integer from 1 to 4, depending on what the last column is that contains any value, formula (or formatting?)

Sort_Active_Book()
	Description: sorts worksheet tabs by their name in either A-Z (ascending) or Z-A (descending) order
	Returns: a sorted workbook
	Use example: Either run a macro from within excel or highlight the function within VBA and Select "Run"->"Run Sub"

ImportCSVs()
	Description: import all .CSVs from a single directory into one workbook in Excel
	Returns: a workbook with a lot of CSV-derived sheets
	Use example: Modify the fPath variable in the function in VBA, highlight and "Run"->"Run Sub"

ToggleMenus()
	Description: turn off ribbon view and bring back the standard menu bar (or reverse this operation)

GetCustomNumberFormats()
	Description: utility function to get up to 1000 Custom Number Formats available to a particular workbook
	Returns: a new sheet in the current workbook with a listing of Custom formats along with an indication of which are presently in use (and which are not)
