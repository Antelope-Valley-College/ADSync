create or replace package body                         ad_provisioning as

--**********************************************************************************
--
--  Package Name:   avc.ad_provisioning.sql
--  Owner:          AVC
--  Author:         Scott Tuss
--                  Antelope Valley College
--  Creation Date:  08 NOV 2018
--
--  Purpose:        Package to build flat files for Active Directory (AD) account
--                  provisioning.
--
--  Dependancies:
--
--
--  Audit Trail
--  ================================================================================
--  DATE          BY   REL    COMMENTS
--  ================================================================================
--  08 NOV 2018   ST   1.0    Original design and implementation completed.
--  13 SEP 2019   ST   1.0.1  Removed middle initial as per RSHAW (replaced with null)
--  13 JAN 2020   ST   1.1    Student workers who are in the EPAF queue are identified
--                            as student workers
--  08 JUN 2020   ST   1.1.1  FIX: Removed courses with STAFF as the assigned instructor.
--                            They caused issues on Mike's side
--  27 MAY 2021   ST   1.1.2  MOD: Added an override to calcRole() so that our contract
--                            VPHR will come out as and EMP and not a CTR.
--  01 JUL 2021   ST   1.1.3  MOD: Added a start and end buffer to the job assignment.
--                            We now catch people before thier start date and give them
--                            some extra time at the end to keep their AD account active.
--  11 AUG 2021   BA   1.1.3b MOD: Modified package to include preferred name 
--                            for active directory. 
--**********************************************************************************

  version           varchar2(10 char) := '1.1.3b';


  -- ===========================================================================
  -- person_list
  -- Loops through all active students and employees to create the person file
  -- ===========================================================================
  procedure person_list is

    v_PersonFile            utl_file.file_type;
    v_OutFilePath           varchar2(50 char) := 'AD_SYNC';
    v_OutFileName           varchar2(256 char):= 'AD_PersonFile.csv';
    v_StartBufferDays       number(2)         := 30; --122121 DC : Changed from 14 to 30
    v_EndBufferDays         number(2)         := 30; --122121 DC: Changed from 14 to 30

    v_Role                  varchar2(6 char)  := null;

    delim                   char              := '|';
    person                  PersonRec;

    -- Get the primary details about a person
    cursor c_PersonDetail (p_pidm   spriden.spriden_pidm%type) is
      select spriden_pidm pidm,
             spriden_id sid,
             baninst1.gokname.f_get_name(spriden_pidm, 'PREF_F') f_name,
             null,
--             substr(spriden_mi,1,1) mi,
             baninst1.gokname.f_get_name(spriden_pidm, 'PREF_L') last_name,
             gobtpac_external_user username,
             gobtpac_external_user||'@avc.edu' email
        from spriden
        join spbpers on spriden_pidm = spbpers_pidm
        join gobtpac on spriden_pidm = gobtpac_pidm
       where spriden_change_ind is null
         and spriden_pidm = p_pidm;

    -- Determine what primary role a person has
    function calcRole (p_pidm   spriden.spriden_pidm%type) return varchar is

      fv_Role       varchar2(6 char)    := 'STU';
      fv_Ntyp       varchar2(4 char)    := null;

      -- Is Contractor
      function f_isCtr return boolean is
        rtnval    boolean := FALSE;
        holdPidm  spriden.spriden_pidm%type := null;

        cursor c_Ctr is
          select spriden_pidm
            from spriden
           where spriden_pidm = p_pidm
             and spriden_change_ind is null
             and spriden_ntyp_code = 'CTR';
      begin
        open c_Ctr;
        fetch c_Ctr into holdPidm;
        if holdPidm is not null then
          rtnVal := TRUE;
        end if;
        close c_Ctr;

        return rtnval;
      end f_isCtr;

      -- Is an employees
      function f_isEmp (p_empType varchar2) return boolean is
        rtnval    boolean := FALSE;
        holdPidm  spriden.spriden_pidm%type := null;

        cursor c_Emp is
          select nbrjobs_pidm pidm
            from pebempl
            join nbrjobs o
              on pebempl_pidm = nbrjobs_pidm
            join nbrbjob
              on pebempl_pidm = nbrbjob_pidm
             and nbrjobs_posn = nbrbjob_posn
           where pebempl_empl_status = 'A'
             and o.nbrjobs_pidm = p_pidm ----237307 11445 --304970 --341840 --12936 --237935 --61483 --
             and o.nbrjobs_status = 'A'
             and trunc(sysdate) between trunc(nbrbjob_begin_date - v_StartBufferDays) and trunc(nvl(nbrbjob_end_date,to_date('31-DEC-2099','DD-MON-YYYY')) + v_EndBufferDays)
             and ((    p_empType = 'STU' and -- Student Emp
                       o.nbrjobs_ecls_code in ('ST','SI')
                   and o.nbrjobs_posn = (select distinct a.nbrjobs_posn
                                           from nbrjobs a,
                                                nbrjobs t
                                          where a.nbrjobs_pidm = o.nbrjobs_pidm
                                            and t.nbrjobs_pidm = o.nbrjobs_pidm
                                            and a.nbrjobs_posn = o.nbrjobs_posn
                                            and t.nbrjobs_posn = o.nbrjobs_posn
                                            and (((a.nbrjobs_pers_chg_date - v_StartBufferDays) <= sysdate and
                                                  a.nbrjobs_status = 'A')
                                            and  ((t.nbrjobs_pers_chg_date + v_EndBufferDays) >= sysdate and
                                                  t.nbrjobs_status = 'T')))
                   and o.nbrjobs_pers_chg_date = (select max(a.nbrjobs_pers_chg_date)
                                                    from nbrjobs a,
                                                         nbrjobs t
                                                   where a.nbrjobs_pidm = o.nbrjobs_pidm
                                                     and t.nbrjobs_pidm = o.nbrjobs_pidm
                                                     and a.nbrjobs_posn = t.nbrjobs_posn
                                                     and (((a.nbrjobs_pers_chg_date - v_StartBufferDays) <= sysdate and
                                                           a.nbrjobs_status = 'A')
                                                     and  ((t.nbrjobs_pers_chg_date + v_EndBufferDays) >= sysdate and
                                                           t.nbrjobs_status = 'T'))))
                  or
                  (    p_empType = 'EMP' and -- Regualar Emp, Hourly
                       o.nbrjobs_ecls_code not in ('RT','ST','SI','SH','PX','PI')
                   and nbrbjob_contract_type = 'P'
                   and nbrjobs_effective_date = (select max(i.nbrjobs_effective_date)
                                                   from nbrjobs i
                                                  where i.nbrjobs_pidm = o.nbrjobs_pidm
                                                    and (i.nbrjobs_effective_date - v_EndBufferDays) <= sysdate
                                                    and i.nbrjobs_posn = nbrbjob_posn
                                                    and i.nbrjobs_status = 'A'))
                  or
                  (    p_empType = 'HRLY' and -- Regualar Emp, Hourly
                       o.nbrjobs_ecls_code in ('SH','PX','PI')
--                   and nbrbjob_contract_type = 'P'
                   and o.nbrjobs_pers_chg_date = (select max(i.nbrjobs_pers_chg_date)
                                                   from nbrjobs i
                                                  where i.nbrjobs_pidm = o.nbrjobs_pidm
                                                    and (i.nbrjobs_pers_chg_date - v_EndBufferDays) <= sysdate
                                                    and i.nbrjobs_posn = nbrbjob_posn)));



/*
          select nbrjobs_pidm pidm
            from pebempl
            join nbrjobs o
              on pebempl_pidm = nbrjobs_pidm
            join nbrbjob
              on pebempl_pidm = nbrbjob_pidm
             and nbrjobs_posn = nbrbjob_posn
           where pebempl_empl_status = 'A'
             and o.nbrjobs_pidm = p_pidm --11445 --304970 --341840 --12936 --237935 --61483 --
             and o.nbrjobs_status = 'A'
             and trunc(sysdate) between trunc(nbrbjob_begin_date) and trunc(nvl(nbrbjob_end_date,to_date('31-DEC-2099','DD-MON-YYYY')))
             and ((    p_empType = 'STU' and -- Student Emp
                       o.nbrjobs_ecls_code in ('ST','SI')
                   and o.nbrjobs_posn = (select distinct a.nbrjobs_posn
                                           from nbrjobs a,
                                                nbrjobs t
                                          where a.nbrjobs_pidm = o.nbrjobs_pidm
                                            and t.nbrjobs_pidm = o.nbrjobs_pidm
                                            and a.nbrjobs_posn = o.nbrjobs_posn
                                            and t.nbrjobs_posn = o.nbrjobs_posn
                                            and ((a.nbrjobs_pers_chg_date <= sysdate and
                                                  a.nbrjobs_status = 'A')
                                            and  (t.nbrjobs_pers_chg_date >= sysdate and
                                                  t.nbrjobs_status = 'T')))
                   and o.nbrjobs_pers_chg_date = (select max(a.nbrjobs_pers_chg_date)
                                                    from nbrjobs a,
                                                         nbrjobs t
                                                   where a.nbrjobs_pidm = o.nbrjobs_pidm
                                                     and t.nbrjobs_pidm = o.nbrjobs_pidm
                                                     and a.nbrjobs_posn = t.nbrjobs_posn
                                                     and ((a.nbrjobs_pers_chg_date <= sysdate and
                                                           a.nbrjobs_status = 'A')
                                                     and  (t.nbrjobs_pers_chg_date >= sysdate and
                                                           t.nbrjobs_status = 'T'))))
                  or
                  (    p_empType = 'EMP' and -- Regualar Emp, Hourly
                       o.nbrjobs_ecls_code not in ('RT','ST','SI','SH','PX','PI')
                   and nbrbjob_contract_type = 'P'
                   and nbrjobs_effective_date = (select max(i.nbrjobs_effective_date)
                                                   from nbrjobs i
                                                  where i.nbrjobs_pidm = o.nbrjobs_pidm
                                                    and i.nbrjobs_effective_date <= sysdate
                                                    and i.nbrjobs_posn = nbrbjob_posn))
                  or
                  (    p_empType = 'HRLY' and -- Regualar Emp, Hourly
                       o.nbrjobs_ecls_code in ('SH','PX','PI')
--                   and nbrbjob_contract_type = 'P'
                   and o.nbrjobs_pers_chg_date = (select max(i.nbrjobs_pers_chg_date)
                                                   from nbrjobs i
                                                  where i.nbrjobs_pidm = o.nbrjobs_pidm
                                                    and i.nbrjobs_pers_chg_date <= sysdate
                                                    and i.nbrjobs_posn = nbrbjob_posn)));
*/

      begin
        open c_Emp;
        fetch c_Emp into holdPidm;
        if holdPidm is not null then
          rtnval := TRUE;
        end if;
        close c_Emp;

        
        return rtnval;
      end f_isEmp;


      function f_inEPAFQueue return boolean is
        rtnval    boolean := FALSE;
        holdPidm  spriden.spriden_pidm%type := null;

        cursor c_EPAF is
          select distinct spriden_pidm pidm
            from ntrinst,
                 ntvacat,
                 spriden,
                 nobtran x
           where ntvacat_code = nobtran_acat_code
             and spriden_pidm (+) = nobtran_pidm
             and spriden_change_ind (+) is null
             and (ntrinst_ea_display_duration is null
                  or nobtran_submission_date is null
                  or trunc(last_day(
                           add_months(
                           nobtran_submission_date,
                           nvl(ntrinst_ea_display_duration, 0))))
                           >= trunc(sysdate))
             and (nobtran_submission_date is null
                  or trunc(nobtran_submission_date)
                     between trunc(to_date('01-JAN-12','DD-MON-YY')) and trunc(to_date('31-DEC-99','DD-MON-YY')))
             and nobtran_transaction_no in (select norrout_transaction_no
                                              from norrout
                                             where norrout_recipient_user_id = 'SBURKHOLDER'
                                               and norrout_queue_status_ind in ('P','F','M'))
             and nokepaf.f_routing_action_ind (
                  'SBURKHOLDER', null, 'APPROVER', 'N', nobtran_transaction_no,
                  '',
                  'P:F:M') = 'A'
             and spriden_pidm = p_pidm;

      begin
        open c_EPAF;
        fetch c_EPAF into holdPidm;
        if holdPidm is not null then
          rtnVal := TRUE;
        end if;
        close c_EPAF;

        return rtnval;
      end f_inEPAFQueue;


    begin
      case
        when f_isEmp('STU')  then fv_Role := 'STUEMP';
        when f_inEPAFQueue   then fv_Role := 'STUEMP';
        when f_isEmp('EMP')  then fv_Role := 'EMP';
        when f_isEmp('HRLY') then fv_Role := 'EMP';
        when f_isCtr         then fv_Role := 'CTR';
        else                      fv_Role := 'STU';
      end case;

--========================================
--NAME:   Benson, Laura
--PIDM:   494468
--CCCID:
--SRC_ID: 392258
--SID :   900398092
--USR :   lbenson4_ess
--========================================

      -- Override for our contract VPHR
      if p_pidm = 494468 then
        fv_Role := 'EMP';
      end if;

      return fv_Role;
    end calcRole;

  begin

    v_PersonFile := utl_file.fopen(v_OutFilePath, v_outFileName, 'w');

    for r_main in c_main loop
      open c_PersonDetail (r_main.pidm);
      fetch c_PersonDetail into person;

      v_Role := CalcRole (r_main.pidm);

      utl_file.put_line(v_PersonFile,
                        v_Role||delim||
                        person.sid||delim||
                        person.first_name||delim||
                        person.mi||delim||
                        person.last_name||delim||
                        person.username||delim||
                        person.email);
      close c_PersonDetail;


    end loop;  -- for r_main in c_main
    
    utl_file.put_line(v_PersonFile,
                    'EMP'||delim||
                    '900418111'||delim||
                    'Jennifer'||delim||
                    'LH'||delim||
                    'Zellet'||delim||
                    'jzellet'||delim||
                    'jzellet@avc.edu');

    utl_file.fflush(v_PersonFile);
    utl_file.fclose(v_PersonFile);

  end person_list;

  procedure course_list is

    v_CourseFile            utl_file.file_type;
    v_OutFilePath           varchar2(50 char) := 'AD_SYNC';
    v_OutFileName           varchar2(256 char):= 'AD_Courses.csv';

    delim                   char              := '|';
--    course                  CourseRec;

  begin
    v_CourseFile := utl_file.fopen(v_OutFilePath, v_outFileName, 'w');
    for r_Courses in c_Courses loop
      utl_file.put_line(v_CourseFile, r_Courses.term_code||delim||
                                      r_Courses.crn||delim||
                                      r_Courses.subj_code||delim||
                                      r_Courses.crse_numb||delim||
                                      r_Courses.seq_numb||delim||
                                      r_Courses.start_date||delim||
                                      r_Courses.end_date);
    end loop;

    utl_file.fflush(v_CourseFile);
    utl_file.fclose(v_CourseFile);

  end course_list;

  procedure faculty_assignment is
    v_FacAssignFile         utl_file.file_type;
    v_OutFilePath           varchar2(50 char) := 'AD_SYNC';
    v_OutFileName           varchar2(256 char):= 'AD_FacAssign.csv';
    v_PrimaryInd            varchar2(1 char)  := null;

    delim                   char              := '|';

    cursor c_FacAssign (p_crn  varchar,
                        p_term varchar) is
      select distinct sirasgn_term_code term_code,
             sirasgn_pidm pidm,
             f_getspridenid(sirasgn_pidm) id,
             sirasgn_crn crn,
             ssbsect_subj_code subj_code,
             ssbsect_crse_numb crse_numb,
             ssbsect_seq_numb seq_numb
      --       sirasgn_primary_ind prim_ind
        from sirasgn
        join ssbsect
          on sirasgn_crn = ssbsect_crn
         and sirasgn_term_code = ssbsect_term_code
       where sirasgn_crn = p_crn
         and sirasgn_term_code = p_term;

    function f_isPrimary (p_crn  varchar,
                          p_term varchar,
                          p_pidm number) return boolean is

      rtnval      boolean          := FALSE;
      hldPrimary  varchar2(1 char) := null;

      cursor c_chkPrimary is
        select sirasgn_primary_ind prim_ind
          from sirasgn
         where sirasgn_term_code = p_term
           and sirasgn_pidm = p_pidm
           and sirasgn_crn = p_crn
           and sirasgn_primary_ind = 'Y';
    begin
      open c_chkPrimary;
      fetch c_chkPrimary into hldPrimary;
      if c_chkPrimary%found then
        rtnval := TRUE;
      end if;
      close c_chkPrimary;

      return rtnval;
    end;
  begin
    v_FacAssignFile := utl_file.fopen(v_OutFilePath, v_outFileName, 'w');

    for r_Courses in c_Courses loop
      for r_FacAssign in c_FacAssign(r_Courses.crn, r_Courses.term_code) loop
        v_PrimaryInd := null;
        if f_isPrimary(r_FacAssign.crn, r_FacAssign.term_code, r_FacAssign.pidm) then
          v_PrimaryInd := 'Y';
        end if;

        if r_FacAssign.id != 'STAFF' then
          utl_file.put_line(v_FacAssignFile, r_FacAssign.term_code||delim||
                                             r_FacAssign.crn||delim||
                                             r_FacAssign.subj_code||delim||
                                             r_FacAssign.crse_numb||delim||
                                             r_FacAssign.seq_numb||delim||
                                             r_FacAssign.id||delim||
                                             v_PrimaryInd);
        end if;
      end loop;
    end loop;

    utl_file.fflush(v_FacAssignFile);
    utl_file.fclose(v_FacAssignFile);

  end faculty_assignment;


  procedure student_enrollment is

    v_StuEnrollFile         utl_file.file_type;
    v_OutFilePath           varchar2(50 char) := 'AD_SYNC';
    v_OutFileName           varchar2(256 char):= 'AD_StuEnroll.csv';
    v_PrimaryInd            varchar2(1 char)  := null;

    delim                   char              := '|';

    cursor c_courseEnrollment (p_term   varchar2,
                               p_crn    number) is

      select sfrstcr_pidm pidm,
             f_getspridenid(sfrstcr_pidm) id,
--             f_format_name(sfrstcr_pidm, 'LF30') name,
             sfrstcr_term_code term_code,
             sfrstcr_crn crn,
--             ssbsect_subj_code subj_code,
--             ssbsect_crse_numb crse_numb,
--             ssbsect_seq_numb seq_numb,
             decode(sfrstcr_grde_code,'I','Y','N') inc_ind
--             sfrstcr_rsts_code rsts
        from sfrstcr
        join stvrsts
          on sfrstcr_rsts_code = stvrsts_code
--        join ssbsect
--          on sfrstcr_term_code = ssbsect_term_code
--         and sfrstcr_crn = ssbsect_crn
       where sfrstcr_term_code = p_term
         and sfrstcr_crn = p_crn
         and stvrsts_voice_type = 'R'
       order by 3;

  begin
    v_StuEnrollFile := utl_file.fopen(v_OutFilePath, v_outFileName, 'w');

    for r_Courses in c_Courses loop
      for r_crseEnrl in c_courseEnrollment(r_Courses.term_code, r_Courses.crn) loop
        utl_file.put_line(v_StuEnrollFile, r_crseEnrl.term_code||delim||
                                           r_crseEnrl.crn||delim||
                                           r_Courses.subj_code||delim||
                                           r_Courses.crse_numb||delim||
                                           r_Courses.seq_numb||delim||
                                           r_crseEnrl.id||delim||
                                           r_crseEnrl.inc_ind);
      end loop;
    end loop;

    utl_file.fflush(v_StuEnrollFile);
    utl_file.fclose(v_StuEnrollFile);

  end student_enrollment;


  procedure employee_job_details is

    v_EmpJobFile            utl_file.file_type;
    v_OutFilePath           varchar2(50 char) := 'AD_SYNC';
    v_OutFileName           varchar2(256 char):= 'AD_EmpJobDetails.csv';

    delim                   char              := '|';

    cursor c_EmployeeJobsDetails is
      select e.nbrjobs_pidm emp_pidm,
             f_getspridenid(e.nbrjobs_pidm) emp_id,
             f_format_name(e.nbrjobs_pidm,'LF30') emp_name,
      e.nbrjobs_posn emp_posn,
             nbbposn_posn_reports sup_posn,
      s.nbrjobs_pidm sup_pidm,
             f_getspridenid(s.nbrjobs_pidm) sup_id,
             f_format_name(s.nbrjobs_pidm,'LF30') sup_name,
             e.nbrjobs_orgn_code_ts orgn_code,
             ftvorgn_title dept,
--             case substr(e.nbrjobs_posn,1,2)
--               when 'FF' then 'Faculty'
--               when 'FA' then 'Adjunct Faculty'
--               else e.nbrjobs_desc
--             end job_title,
             e.nbrjobs_desc job_title,
             nbrbjob_contract_type pri_ind
        from nbrjobs e
        join nbbposn on nbrjobs_posn = nbbposn_posn
        join nbrjobs s on s.nbrjobs_posn = nbbposn_posn_reports
        join ftvorgn f on e.nbrjobs_orgn_code_ts = f.ftvorgn_orgn_code
        join nbrbjob on nbrbjob_pidm = e.nbrjobs_pidm
         and            nbrbjob_posn = e.nbrjobs_posn
       where e.nbrjobs_status = 'A'
         and s.nbrjobs_status = 'A'
      --   and e.nbrjobs_pidm = 18444 --103487 --291436 --28698 --121955 --122290  --18444
         and e.nbrjobs_ecls_code not in('PI','PX','RT','SI','ST')
         and e.nbrjobs_effective_date = (select max(i.nbrjobs_effective_date)
                                           from nbrjobs i
                                          where i.nbrjobs_pidm = e.nbrjobs_pidm
      --                                      and i.nbrjobs_status = 'A'
                                            and i.nbrjobs_effective_date <= sysdate)
         and s.nbrjobs_effective_date = (select max(i.nbrjobs_effective_date)
                                           from nbrjobs i
                                          where i.nbrjobs_pidm = s.nbrjobs_pidm
      --                                      and i.nbrjobs_status = 'A'
                                            and i.nbrjobs_effective_date <= sysdate)
         and f.ftvorgn_eff_date = (select min(i.ftvorgn_eff_date)
                                     from ftvorgn i
                                    where sysdate between i.ftvorgn_eff_date and i.ftvorgn_nchg_date
                                      and i.ftvorgn_orgn_code = f.ftvorgn_orgn_code)
         and nbrbjob_contract_type = 'P' --<> 'O'
       order by sup_name,
                emp_name;

    function f_deptCleanUp (p_dept varchar2) return varchar2 is
      holdDept    varchar2(50 char) := null;
    begin
      holdDept := p_dept;
      -- Replace abbriviation for Research
      holdDept := replace(holdDept, 'Rsrch', 'Research');

      -- Remove "Instruction" and it's abbriviations from department names
      -- but leave "Instructional Multimedia Center" alone.
      case
        when instr(holdDept, 'Instruction') > 1 then holdDept := replace(holdDept, 'Instruction', null);
        else null;
      end case;
      holdDept := replace(holdDept, 'Inst.', null);

      -- Add spacees around '&' in "Health&Safety Sciences"
      holdDept := replace(holdDept, 'Health', 'Health ');

      -- Replace abbriviation for Education
      holdDept := replace(holdDept, 'Corporate  Ed', 'Corporate  Education');

      -- Add and 's' to "Information Technology Service"
      holdDept := replace(holdDept, 'Information Technology Service', 'Information Technology Services');

      return trim(holdDept);
    end;

  begin
    v_EmpJobFile := utl_file.fopen(v_OutFilePath, v_outFileName, 'w');

    for r_EmpJob in c_EmployeeJobsDetails loop
      utl_file.put_line(v_EmpJobFile,r_EmpJob.emp_id||delim||
                                     rpad(r_EmpJob.emp_name,30,' ')||delim||
rpad(r_EmpJob.emp_posn,8,' ')||delim||
                                     r_EmpJob.sup_id||delim||
                                     rpad(r_EmpJob.sup_name,30,' ')||delim||
rpad(r_EmpJob.sup_posn,8,' ')||delim||
--                                     f_deptCleanUp(r_EmpJob.dept)||delim||
                                     rpad(r_EmpJob.dept,50,' ')||delim||
                                     r_EmpJob.job_title);
    end loop;

    utl_file.fflush(v_EmpJobFile);
    utl_file.fclose(v_EmpJobFile);

  end employee_job_details;

end ad_provisioning;