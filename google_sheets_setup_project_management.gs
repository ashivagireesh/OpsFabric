function setupProjectManagementSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const lists = {
    projectStatus: ['Not Started','Active','On Hold','Completed','Delayed','Cancelled'],
    taskStatus: ['Not Started','In Progress','Blocked','Completed','Delayed','Deferred'],
    milestoneStatus: ['Not Started','In Progress','Completed','Delayed','At Risk'],
    issueStatus: ['Open','In Progress','Resolved','Closed','Reopened'],
    priority: ['Low','Medium','High','Critical'],
    riskLevel: ['Low','Medium','High','Critical'],
    severity: ['Low','Medium','High','Critical'],
    approvalStatus: ['Pending','Approved','Rejected','Deferred'],
    testResult: ['Not Run','Pass','Fail','Blocked'],
    retestStatus: ['Pending','Pass','Fail','Not Required'],
    deploymentStatus: ['Planned','Successful','Failed','Rolled Back','Completed'],
    yesNo: ['Yes','No'],
    phase: ['Initiation','Requirements','Design','Development','Testing','UAT','Deployment','Hypercare','Closed'],
    environment: ['Development','SIT','UAT','Pre-Production','Production','DR'],
    impactProbability: ['1','2','3','4','5'],
    people: ['Priya Nair','Arjun Patel','Leena Das','Nikhil Verma','Imran Ali','Riya Sen','Sara Thomas','Ananya Rao','Rohit Menon','Meera Iyer','Vikram Shah'],
    projectIds: ['PRJ-001','PRJ-002','PRJ-003','PRJ-004']
  };
  const rule = values => SpreadsheetApp.newDataValidation().requireValueInList(values, true).setAllowInvalid(false).build();
  const apply = (sheetName, colMap, rows) => {
    const sh = ss.getSheetByName(sheetName);
    if (!sh) return;
    const existing = sh.getFilter();
    if (existing) existing.remove();
    sh.getRange(1, 1, rows, sh.getLastColumn()).createFilter();
    Object.entries(colMap).forEach(([col, values]) => sh.getRange(`${col}2:${col}${rows}`).setDataValidation(rule(values)));
    sh.setFrozenRows(1);
  };
  apply('Project Master', {A: lists.projectIds, D: lists.people, I: lists.projectStatus, J: lists.priority, L: lists.phase, M: lists.riskLevel}, 300);
  apply('Task Tracker', {B: lists.projectIds, E: lists.people, I: lists.taskStatus, J: lists.priority}, 500);
  apply('Milestone Tracker', {B: lists.projectIds, F: lists.milestoneStatus, H: lists.people}, 300);
  apply('Issue Tracker', {B: lists.projectIds, F: lists.people, G: lists.severity, H: lists.issueStatus}, 300);
  apply('Risk Register', {B: lists.projectIds, D: lists.impactProbability, E: lists.impactProbability, G: lists.riskLevel, I: lists.people, J: lists.issueStatus}, 300);
  apply('Resource Tracker', {A: lists.people, C: lists.projectIds}, 300);
  apply('Change Request Tracker', {B: lists.projectIds, H: lists.approvalStatus, J: lists.taskStatus}, 300);
  apply('UAT Testing Tracker', {B: lists.projectIds, G: lists.testResult, I: lists.retestStatus}, 300);
  apply('Deployment Tracker', {B: lists.projectIds, C: lists.environment, G: lists.deploymentStatus, H: lists.yesNo}, 200);
  const dash = ss.getSheetByName('Project Dashboard');
  if (dash) {
    dash.getRange('B3').setDataValidation(rule(['All', ...lists.projectIds]));
    dash.getRange('E3').setDataValidation(rule(['All', ...lists.projectStatus]));
    dash.getRange('H3').setDataValidation(rule(['All', ...lists.priority]));
    dash.getRange('K3').setDataValidation(rule(['All', ...lists.riskLevel]));
  }
}
