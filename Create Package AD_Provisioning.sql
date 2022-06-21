create or replace package         ad_provisioning as

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
--  18 AUG 2021   ST   1.1    BUG FIX: Corrected the subquery for current employees.
--                            v_EndBufferDays was being added to the job effective day,
--                            pushing it further out delaying the creation of the AD account.
--                            The fix was to add the buffer to sysdate so that we were 
--                            looking X days into the future.
--  16 JUN 2022   DC   1.2    The + v_EndBufferDays logic deprovisioned many users. Added a between date condition to fix this. Old row count matches new row count
--**********************************************************************************
  v_EndBufferDays       number(2) := 14;

  cursor c_main is
    select distinct pidm
      from (select nbrjobs_pidm pidm  -- employees and student workers
              from nbrjobs o
             where o.nbrjobs_status = 'A'
               and o.nbrjobs_ecls_code <> 'RT' -- retired
               and o.nbrjobs_effective_date between (select max(i.nbrjobs_effective_date)
                                                 from nbrjobs i
                                                where i.nbrjobs_pidm = o.nbrjobs_pidm
                                                  and (i.nbrjobs_effective_date) <= sysdate )
                                                  
                                                  and
                                                  
                                                  (select max(i.nbrjobs_effective_date)
                                                 from nbrjobs i
                                                where i.nbrjobs_pidm = o.nbrjobs_pidm
                                                  and (i.nbrjobs_effective_date) <= sysdate + v_EndBufferDays)
                                                  
                                                  
            union
            select distinct spriden_pidm    -- contractors
              from spriden
             where spriden_change_ind is null
               and spriden_ntyp_code = 'CTR'
            union
            select distinct spriden_pidm pidm   -- Sherri's EPAF queue
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
            union
            select distinct sfrstcr_pidm pidm   -- enrolled students in current/future terms
              from sfrstcr
              join stvrsts on sfrstcr_rsts_code = stvrsts_code
             where stvrsts_voice_type = 'R'
               and sfrstcr_term_code in (select sfrrsts_term_code
                                           from sfrrsts
                                           join stvterm on (stvterm_code = sfrrsts_term_code)
                                           join goriccr on (goriccr_value = sfrrsts_term_code)
                                          where sfrrsts_rsts_code = 'RE'
                                            and sfrrsts_ptrm_code = '1'
                                            and (sysdate between sfrrsts_start_date and stvterm_start_date OR
                                                 sysdate between stvterm_start_date and stvterm_end_date)
                                            and goriccr_sqpr_code = 'ELEARNING'
                                            and goriccr_icsn_code = 'ACTIVE_TERM')
            union
            -- 2nd SELECT gets everyone enrolled in a class that has not completed yet.
            -- The main purpose is to catch students enrolled in classes the extend past
            -- the normal end of term.
            -- There will be overlap between the 1st and 2nd quiries, that's OK.
            select distinct sfrstcr_pidm pidm
              from sfrstcr
              join stvrsts on sfrstcr_rsts_code = stvrsts_code
             where stvrsts_voice_type = 'R'
               and sfrstcr_term_code in (select stvterm_code
                                           from stvterm
                                          where (add_months(sysdate, -6) < stvterm_end_date
                                            and sysdate > stvterm_start_date))
               and sfrstcr_crn in (select ssbsect_crn crn
                                     from ssbsect,
                                          stvterm
                                    where stvterm_code = ssbsect_term_code
                                      and ssbsect_ptrm_end_date > stvterm_end_date
                                      and stvterm_code in (select stvterm_code
                                                             from stvterm
                                                            where (add_months(sysdate, -6) < stvterm_end_date
                                                              and sysdate > stvterm_start_date))
                                      and ssbsect_ptrm_end_date > sysdate
                                      and stvterm_start_date <= sysdate));

    cursor c_Courses is
      select ssbsect_term_code term_code,
             ssbsect_crn crn,
             ssbsect_subj_code subj_code,
             ssbsect_crse_numb crse_numb,
             ssbsect_seq_numb seq_numb,
             to_char(ssbsect_ptrm_start_date,'DD-MON-YYYY') start_date,
             to_char(ssbsect_ptrm_end_date,'DD-MON-YYYY') end_date
        from ssbsect
       where ssbsect_ssts_code = 'A'
         and ((trunc(sysdate) between trunc(ssbsect_ptrm_start_date) and    -- current courses
                                      trunc(ssbsect_ptrm_end_date)+366) or  -- courses offers in the past year
              (ssbsect_term_code in (select sfrrsts_term_code   -- Current term, and future terms
                                           from sfrrsts         -- that are open for registration
                                           join stvterm on (stvterm_code = sfrrsts_term_code)
                                           join goriccr on (goriccr_value = sfrrsts_term_code)
                                          where sfrrsts_rsts_code = 'RE'
                                            and sfrrsts_ptrm_code = '1'
                                            and (sysdate between sfrrsts_start_date and stvterm_start_date OR
                                                 sysdate between stvterm_start_date and stvterm_end_date)
                                            and goriccr_sqpr_code = 'ELEARNING'
                                            and goriccr_icsn_code = 'ACTIVE_TERM')))
       order by ssbsect_term_code,
                ssbsect_subj_code,
                ssbsect_crse_numb,
                ssbsect_seq_numb;

  type PersonRec is record (
    pidm              spriden.spriden_pidm%type           := null,
    sid               spriden.spriden_id%type             := null,
    first_name        spriden.spriden_first_name%type     := null,
    mi                spriden.spriden_mi%type             := null,
    last_name         spriden.spriden_last_name%type      := null,
    username          gobtpac.gobtpac_external_user%type  := null,
    email             goremal.goremal_email_address%type  := null);
--    role              varchar2(10 char)                   := null);

  type CourseRec is record (
    term_code         ssbsect.ssbsect_term_code%type      := null,
    crn               ssbsect.ssbsect_crn%type            := null,
    subj_code         ssbsect.ssbsect_subj_code%type      := null,
    crse_numb         ssbsect.ssbsect_crse_numb%type      := null,
    seq_numb          ssbsect.ssbsect_seq_numb%type       := null,
    start_date        ssbsect.ssbsect_ptrm_start_date%type := null,
    end_date          ssbsect.ssbsect_ptrm_end_date%type  := null);

  procedure person_list;

  procedure course_list;

  procedure faculty_assignment;

  procedure student_enrollment;

  procedure employee_job_details;

end ad_provisioning;