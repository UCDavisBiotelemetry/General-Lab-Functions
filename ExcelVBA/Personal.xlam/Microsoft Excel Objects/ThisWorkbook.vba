Public WithEvents App As Application
Public Wb As Workbook

Private Sub Workbook_Open()
    Set App = Application
    Set Wb = App.ActiveWorkbook
    Call App_WorkbookOpen(Wb)
End Sub

Private Sub App_WorkbookOpen(ByVal Wb As Workbook)
    Set App = Application
    If Wb Is Nothing Then Set Wb = Application.ActiveWorkbook
    If Wb Is Nothing Then Exit Sub
    code = Wb.FileFormat
    Select Case code
        Case xlCurrentPlatformText, xlCSV, xlCSVMac, xlCSVMSDOS, xlCSVWindows, xlTextMac, xlTextMSDOS, xlTextWindows, xlUnicodeText
            Call TemplateCSVs(Wb)
        Case Else
            If Right(Wb.Name, 2) = "sv" Then ' e.g. csv or tsv
                Call TemplateCSVs(Wb)
            End If
    End Select
    Wb.Saved = True ' clear dirty flag that this VBA inadvertantly set
End Sub

Private Function BuildDTString(DateTimeType As Integer)
    i = Application.International
    sDate = ""
    sTime = ""
    If (DateTimeType Mod 2) = 1 Then 'date = yes
        y = String((Abs(i(xl4DigitYears)) + 1) * 2, i(xlYearCode))
        m = String(Abs(i(xlMonthLeadingZero)) + 1, i(xlMonthCode))
        d = String(Abs(i(xlDayLeadingZero)) + 1, i(xlDayCode))
        j = i(xlDateSeparator)
        Select Case i(xlDateOrder)
            Case 0 'm-d-y
                sDate = m & j & d & j & y
            Case 1 'd-m-y
                sDate = d & j & m & j & y
            Case Else '2=y-m-d
                sDate = y & j & m & j & d
        End Select
    End If
    If DateTimeType > 1 Then '2-5. time = yes
        h = String(Abs(i(xlTimeLeadingZero)) + 1, i(xlHourCode))
        n = String(2, i(xlMinuteCode))
        s = String(2, i(xlSecondCode))
        j = i(xlTimeSeparator)
        If DateTimeType > 3 Then ms = ".000" Else ms = ""
        If i(xl24HourClock) Then a = "" Else a = " AM/PM"
        sTime = h & j & m & j & s & ms & a
    End If
    BuildDTString = Trim(sDate & " " & sTime)
End Function


Private Sub TemplateCSVs(ByVal Wb As Workbook)
'set up source and target formats
    s1 = "d-mmm"
    s2a = "yyyy-mm-dd hh:mm"
    s2b = "m/d/yyyy h:mm"
    t1 = BuildDTString(1) ' "yyyy-mm-dd" or mm/dd/yy, etc depending on user's control panel settings
    t2 = BuildDTString(3) ' "yyyy-mm-dd hh:mm:ss [AM/PM]"
    t3 = BuildDTString(5) ' "yyyy-mm-dd hh:mm:ss.000 [AM/PM]"
    
'add quick-buttons for these formats
    Wb.Styles.Add("DateOnly").NumberFormat = t1
    Wb.Styles.Add("DateTime").NumberFormat = t2
    Wb.Styles.Add("DateTime.ms").NumberFormat = t3
    
'find out what format these two cells have specifically
    nf1 = Cells.Range("A1").NumberFormat
    nf2 = Cells.Range("B1").NumberFormat
    
'at the end, when setting back the formats in these cells, make sure to update in accord with the other cells
    If nf2 = s2a Then nf2 = t2
    If nf2 = s2b Then nf2 = t2
    If nf1 = s1 Then nf1 = t1
    On Error GoTo n1 'jump to next block
    Cells.Range("A1").NumberFormat = s2a
    Cells.Range("B1").NumberFormat = t2
    Wb.Application.FindFormat.NumberFormat = s2a
    Wb.Application.ReplaceFormat.NumberFormat = t2
    Cells.Replace "", "", xlPart, , , , True, True
    Wb.Application.FindFormat.NumberFormat = s2b 'this could throw an error, but if it does, we just skip to the next block
    Cells.Replace "", "", xlPart, , , , True, True
nun1:
    On Error GoTo n2 ' this shouldn't happen...but if it does...
    Cells.Range("A1").NumberFormat = s1
    Cells.Range("B1").NumberFormat = t1
    Wb.Application.FindFormat.NumberFormat = s1
    Wb.Application.ReplaceFormat.NumberFormat = s1
    Cells.Replace "", "", xlPart, , , , True, True
nun2:
    On Error GoTo 0 'resume standard error behavior
    Cells.Range("A1").NumberFormat = nf1 'set cells back
    Cells.Range("B1").NumberFormat = nf2
    Wb.Application.FindFormat.Clear
    Wb.Application.ReplaceFormat.Clear
    Wb.ActiveSheet.Columns.AutoFit
    Exit Sub
n1:
    Resume nun1
n2:
    Resume nun2
End Sub
