Function CSVifyArray(inRng As Range, Optional sep As String = ",", Optional ColFirst As Boolean = False) As String
Dim i As Long, j As Long, ib As Long, ie As Long, jb As Long, je As Long
Dim rText As String
Dim item As String
Dim addStr As String
Dim pArray As Variant
pArray = inRng.Value
If ColFirst = False Then
    ib = LBound(pArray, 1) 'i holds row indexes
    ie = UBound(pArray, 1)
    jb = LBound(pArray, 2) 'j holds column indexes
    je = UBound(pArray, 2)
Else 'Rem PivotTable-like implementation
    jb = LBound(pArray, 1) 'REM j will hold row indexes
    je = UBound(pArray, 1)
    ib = LBound(pArray, 2) 'REM i holds column indexes
    ie = UBound(pArray, 2)
End If
rText = ""
    For i = ib To ie
        For j = jb To je
            If Not (ColFirst) Then item = CStr(pArray(i, j)) Else item = CStr(pArray(j, i))
            If (item = "0") Then item = "" 'Rem this was so blank cells do not contribute.  If "0" is a legitimate value for you, you may need additional (or less) logic here
            If (rText <> "") Then addStr = sep & item Else addStr = item
            If (item <> "") Then rText = rText & addStr
        Next j
    Next i
CSVifyArray = rText
End Function 'CSVifyArray

Function CSVifyArStrings(inRng As Range, Optional quotechar As String = "'", Optional sep As String = ",", Optional ColFirst As Boolean = False) As String
Dim i As Long, j As Long, ib As Long, ie As Long, jb As Long, je As Long
Dim rText As String
Dim item As String
Dim addStr As String
Dim pArray As Variant
pArray = inRng.Value
If ColFirst = False Then
    ib = LBound(pArray, 1) 'i holds row indexes
    ie = UBound(pArray, 1)
    jb = LBound(pArray, 2) 'j holds column indexes
    je = UBound(pArray, 2)
    If (ie > 1000000) Then ie = LastRow(pArray)
    If (je > 1000000) Then je = LastCol(pArray)
Else 'Rem PivotTable-like implementation
    jb = LBound(pArray, 1) 'REM j will hold row indexes
    je = UBound(pArray, 1)
    ib = LBound(pArray, 2) 'REM i holds column indexes
    ie = UBound(pArray, 2)
    If (je > 1000000) Then je = LastRow(pArray)
    If (ie > 1000000) Then ie = LastCol(pArray)
End If
rText = ""
    For i = ib To ie
        For j = jb To je
            If Not (ColFirst) Then item = CStr(pArray(i, j)) Else item = CStr(pArray(j, i))
            If (item <> "") Then item = quotechar & item & quotechar
            If (rText <> "") Then addStr = sep & item Else addStr = item
            rText = rText & addStr
        Next j
    Next i
CSVifyArStrings = rText
End Function 'CSVifyArStrings

Function LastRow(pArray As Variant)
    Dim r As Long
    Dim rm As Long
    rm = 0
    For r = LBound(pArray, 1) To UBound(pArray, 1)
        If (Not (IsEmpty(pArray(r, 1)))) Then rm = r
    Next r
    LastRow = rm
End Function 'LastRow

Function LastCol(pArray As Variant)
    Dim c As Long
    Dim cm As Long
    cm = 0
    For c = LBound(pArray, 2) To UBound(pArray, 2)
        If (Not (IsEmpty(pArray(1, c)))) Then cm = c
    Next c
    LastCol = cm
End Function 'LastCol

Function MakePair(inRng As Range, ParamArray strs() As Variant)
Dim i As Long
sep = ","
pre = "("
suf = ")"
Dim pArray As Variant
pArray = inRng.Value
Dim rText As String
Dim rEntry As String
rText = ""
For i = LBound(pArray, 2) To UBound(pArray, 2)
    If (strs(i - 1) = True) Then rEntry = "'" & CStr(pArray(1, i)) & "'" Else rEntry = CStr(pArray(1, i))
    If (rText <> "") Then rText = rText & sep & rEntry Else rText = pre & rEntry
Next i
MakePair = rText & suf
End Function

Sub Sort_Active_Book()
Dim i As Integer
Dim j As Integer
Dim iAnswer As VbMsgBoxResult
'
' Prompt the user as which direction they wish to
' sort the worksheets.
'
   iAnswer = MsgBox("Sort Sheets in Ascending Order?" & Chr(10) _
     & "Clicking No will sort in Descending Order", _
     vbYesNoCancel + vbQuestion + vbDefaultButton1, "Sort Worksheets")
   For i = 1 To Sheets.Count
      For j = 1 To Sheets.Count - 1
'
' If the answer is Yes, then sort in ascending order.
'
         If iAnswer = vbYes Then
            If UCase$(Sheets(j).Name) > UCase$(Sheets(j + 1).Name) Then
               Sheets(j).Move after:=Sheets(j + 1)
            End If
'
' If the answer is No, then sort in descending order.
'
         ElseIf iAnswer = vbNo Then
            If UCase$(Sheets(j).Name) < UCase$(Sheets(j + 1).Name) Then
               Sheets(j).Move after:=Sheets(j + 1)
            End If
         End If
      Next j
   Next i
End Sub

Sub ImportCSVs()
'Author:    Jerry Beaucaire
'Date:      8/16/2010
'Summary:   Import all CSV files from a folder into separate sheets
'           named for the CSV filenames
'Update:    2/8/2013   Macro replaces existing sheets if they already exist in master workbook

Dim fPath   As String
Dim fCSV    As String
Dim wbCSV   As Workbook
Dim wbMST   As Workbook

Set wbMST = ActiveWorkbook
fPath = "Z:\Shared\Database\Netronix\DBs\CFTC\69kHz\"                  'path to CSV files, include the final \
Application.ScreenUpdating = False  'speed up macro
Application.DisplayAlerts = False   'no error messages, take default answers
fCSV = Dir(fPath & "*.csv")         'start the CSV file listing
    On Error Resume Next
    Do While Len(fCSV) > 0
        Set wbCSV = Workbooks.Open(fPath & fCSV)                    'open a CSV file
        wbMST.Sheets(ActiveSheet.Name).Delete                       'delete sheet if it exists
        ActiveSheet.Move after:=wbMST.Sheets(wbMST.Sheets.Count)    'move new sheet into Mstr
        Columns.AutoFit             'clean up display
        fCSV = Dir                  'ready next CSV
    Loop
 
Application.ScreenUpdating = True
Set wbCSV = Nothing
End Sub

Sub ToggleMenus()
    Application.DisplayExcel4Menus = Not Application.DisplayExcel4Menus
End Sub

Sub GetCustomNumberFormats()
    Dim Buffer As Object
    Dim Sh As Object
    Dim SaveFormat As Variant
    Dim fFormat As Variant
    Dim nFormat() As Variant
    Dim xFormat As Long
    Dim Counter As Long
    Dim Counter1 As Long
    Dim Counter2 As Long
    Dim StartRow As Long
    Dim EndRow As Long
    Dim Dummy As Variant
    Dim pPresent As Boolean
    Dim NumberOfFormats As Long
    Dim Answer
    Dim c As Object
    Dim DataStart As Long
    Dim DataEnd As Long
    Dim AnswerText As String

    NumberOfFormats = 1000
    ReDim nFormat(0 To NumberOfFormats)
'    AnswerText = "Do you want to delete unused custom formats from the workbook?"
'    AnswerText = AnswerText & Chr(10) & "To get a list of used and unused formats only, choose No."
'    Answer = MsgBox(AnswerText, 259)
'    If Answer = vbCancel Then GoTo Finito
    Answer = vbNo
    On Error GoTo Finito
    Worksheets.Add.Move after:=Worksheets(Worksheets.Count)
    Worksheets(Worksheets.Count).Name = "CustomFormats"
    Worksheets("CustomFormats").Activate
    Set Buffer = Range("A2")
    Buffer.Select
    nFormat(0) = Buffer.NumberFormatLocal
    Counter = 1
    Do
        SaveFormat = Buffer.NumberFormatLocal
        Dummy = Buffer.NumberFormatLocal
        DoEvents
        SendKeys "{tab 3}{down}{enter}"
        Application.Dialogs(xlDialogFormatNumber).Show Dummy
        nFormat(Counter) = Buffer.NumberFormatLocal
        Counter = Counter + 1
    Loop Until nFormat(Counter - 1) = SaveFormat

    ReDim Preserve nFormat(0 To Counter - 2)

    Range("A1").Value = "Custom formats"
    Range("B1").Value = "Formats used in workbook"
    Range("C1").Value = "Formats not used"
    Range("A1:C1").Font.Bold = True

    StartRow = 3
    EndRow = 16384

    For Counter = 0 To UBound(nFormat)
        Cells(StartRow, 1).Offset(Counter, 0).NumberFormatLocal = nFormat(Counter)
        Cells(StartRow, 1).Offset(Counter, 0).Value = nFormat(Counter)
    Next Counter

    Counter = 0
    For Each Sh In ActiveWorkbook.Worksheets
        If Sh.Name = "CustomFormats" Then Exit For
        For Each c In Sh.UsedRange.Cells
            fFormat = c.NumberFormatLocal
            If Application.WorksheetFunction.CountIf(Range(Cells(StartRow, 2), Cells(EndRow, 2)), fFormat) = 0 Then
                Cells(StartRow, 2).Offset(Counter, 0).NumberFormatLocal = fFormat
                Cells(StartRow, 2).Offset(Counter, 0).Value = fFormat
                Counter = Counter + 1
            End If
        Next c
    Next Sh

    xFormat = Range(Cells(StartRow, 2), Cells(EndRow, 2)).Find("").Row - 2
    Counter2 = 0
    For Counter = 0 To UBound(nFormat)
        pPresent = False
        For Counter1 = 1 To xFormat
            If nFormat(Counter) = Cells(StartRow, 2).Offset(Counter1, 0).NumberFormatLocal Then
                pPresent = True
            End If
        Next Counter1
        If pPresent = False Then
            Cells(StartRow, 3).Offset(Counter2, 0).NumberFormatLocal = nFormat(Counter)
            Cells(StartRow, 3).Offset(Counter2, 0).Value = nFormat(Counter)
            Counter2 = Counter2 + 1
        End If
    Next Counter
    With ActiveSheet.Columns("A:C")
        .AutoFit
        .HorizontalAlignment = xlLeft
    End With
    If Answer = vbYes Then
        DataStart = Range(Cells(1, 3), Cells(EndRow, 3)).Find("").Row + 1
        DataEnd = Cells(DataStart, 3).Resize(EndRow, 1).Find("").Row - 1
        On Error Resume Next
        For Each c In Range(Cells(DataStart, 3), Cells(DataEnd, 3)).Cells
            ActiveWorkbook.DeleteNumberFormat (c.NumberFormat)
        Next c
    End If
Finito:
    Set c = Nothing
    Set Sh = Nothing
    Set Buffer = Nothing
End Sub

