"""GAPCC patient pre-registration, portal activation, and booking flow."""

import base64
import asyncio
import hmac
import io
import logging
import os
import re
import secrets
import smtplib
import sqlite3
import ssl
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from email.message import EmailMessage
from cryptography.fernet import Fernet, InvalidToken
from fastapi.middleware.gzip import GZipMiddleware
from hashlib import sha256
from html import escape
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

@asynccontextmanager
async def app_lifespan(application):
    recovered_messages = await asyncio.to_thread(requeue_interrupted_email_notifications)
    if recovered_messages:
        logging.getLogger(__name__).info(
            "Recovered %s interrupted email notifications for retry.",
            recovered_messages,
        )
    dispatcher_task = asyncio.create_task(appointment_email_dispatch_loop())
    application.state.appointment_email_dispatcher = dispatcher_task
    try:
        yield
    finally:
        dispatcher_task.cancel()
        try:
            await dispatcher_task
        except asyncio.CancelledError:
            pass
        application.state.appointment_email_dispatcher = None


app = FastAPI(lifespan=app_lifespan)
DATABASE_PATH = Path(
    os.getenv("GAPCC_DATABASE_PATH") or Path(__file__).with_name("gapcc.db")
)
APPOINTMENT_CAPACITY = 75
MAX_PROFILE_PICTURE_BYTES = 5 * 1024 * 1024
APPOINTMENT_TIME_OPTIONS = (
    "Clinic hours: 8:00 AM - 3:00 PM",
    "OPD hours: 8:00 AM - 4:00 PM",
    "Emergency / Urgent Care: 24 hours / 7 days",
)
app.mount("/assets", StaticFiles(directory=Path(__file__).parent), name="assets")


@app.middleware("http")
async def replace_drug_test_image(request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path in {"/admin", "/doctor", "/portal"} or path.startswith(("/admin/", "/doctor/", "/portal/")):
        return response
    if response.headers.get("content-type", "").startswith("text/html"):
        body = b"".join([chunk async for chunk in response.body_iterator])
        with connect_db() as package_connection:
            managed_packages = package_connection.execute("SELECT package_key, package_name, package_details FROM clinic_packages WHERE active = 1 ORDER BY package_id").fetchall()
        package_defaults = {"health": b"Health Test", "preemployment": b"Pre-employment", "specific": b"Specific Health", "ihc": b"IHC Panel", "autoimmune": b"Autoimmune"}
        package_buttons = []
        package_details = []
        for package in managed_packages:
            default_label = package_defaults.get(package["package_key"])
            if default_label:
                body = body.replace(default_label, package["package_name"].split(" Packages")[0].encode())
            safe_key = escape(package["package_key"], quote=True)
            safe_name = escape(package["package_name"])
            package_buttons.append(
                f'<button type="button" data-package-key="{safe_key}" aria-expanded="false" aria-controls="package-details-{safe_key}">{safe_name}</button>'
            )
            detail_paragraphs = "".join(
                f"<p>{escape(detail.strip())}</p>"
                for detail in package["package_details"].split("|")
                if detail.strip()
            )
            if not detail_paragraphs:
                detail_paragraphs = "<p>Package details are not available yet.</p>"
            package_details.append(
                f'<div class="test-package-details" id="package-details-{safe_key}" hidden><strong>{safe_name}</strong>{detail_paragraphs}</div>'
            )
        package_picker_markup = (
            '<style>.test-package-details{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin-top:16px;padding:16px;background:#f7faf9;border:1px solid #dcebed;border-radius:10px}.test-package-details[hidden]{display:none!important}.test-package-details strong{grid-column:1/-1;color:#07566a;font-size:16px}.test-package-details p{margin:0;padding:12px;background:#fff;border:1px solid #dcebed;border-radius:8px;color:#405c63;font-size:13px;line-height:1.6}.package-options button[aria-expanded="true"]{outline:3px solid #07566a;outline-offset:2px}</style>'
            '<section class="package-picker"><h3>What package are you looking for?</h3>'
            '<div class="package-options">'
            + "".join(package_buttons)
            + '</div><div class="package-details-list">'
            + "".join(package_details)
            + '</div></section><p style="display:none">'
        )
        body = body.replace(
            b"https://images.unsplash.com/photo-1582719478250-c89cae4dc85b?auto=format&fit=crop&w=900&q=82",
            b"/assets/810319289_39758333547099033_8912087341618590151_n.jpg",
        )
        body = body.replace(
            b"https://images.unsplash.com/photo-1579154204601-01588f351e67?auto=format&fit=crop&w=900&q=82",
            b"/assets/813824845_1378616194436907_2471595001531605655_n.jpg",
        )
        body = body.replace(
            b"https://images.unsplash.com/photo-1579154204601-01588f351e67?auto=format&amp;fit=crop&amp;w=900&amp;q=82",
            b"/assets/813824845_1378616194436907_2471595001531605655_n.jpg",
        )
        body = body.replace(
            b"https://images.unsplash.com/photo-1551076805-e1869033e561?auto=format&amp;fit=crop&amp;w=900&amp;q=82",
            b"/assets/810930553_1107251255074583_5747123389390835530_n.jpg",
        )
        body = body.replace(
            b"https://images.unsplash.com/photo-1511174511562-5f7f18b874f8?auto=format&amp;fit=crop&amp;w=900&amp;q=82",
            b"/assets/813993776_2022460808406515_7703188509329161609_n.jpg",
        )
        body = re.sub(
            rb"https://images\.unsplash\.com/photo-1511174511562-5f7f18b874f8[^\"']*",
            b"/assets/813993776_2022460808406515_7703188509329161609_n.jpg",
            body,
        )
        body = re.sub(
            rb"https://images\.unsplash\.com/photo-1516841273335-e39b37888115[^\"']*",
            b"/assets/0c940b19f09b3dfb449be9f2b92bb953.jpg",
            body,
        )
        body = re.sub(
            rb"https://images\.unsplash\.com/photo-1606811971618-4486d14f3f99[^\"']*",
            b"/assets/ccfd5d40b9a0859f0d7209e5f7006edb.jpg",
            body,
        )
        body = body.replace(
            b"https://images.unsplash.com/photo-1579684385127-1ef15d508118?auto=format&amp;fit=crop&amp;w=900&amp;q=82",
            b"/assets/5364922d95c1661732fe35b928fdbc89.jpg",
        )
        body = body.replace(
            b"</head>",
            b"<style>.managed-package-details{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin:18px 0;padding:16px;background:#f7faf9;border:1px solid #dcebed;border-radius:10px}.managed-package-details>strong{grid-column:1/-1;color:#07566a;font-size:18px}.managed-package-details p{margin:0;padding:15px;background:#fff;border:1px solid #dcebed;border-radius:8px;color:#405c63;font-size:12px;line-height:1.7;box-shadow:0 5px 12px rgba(8,26,34,.04)}.managed-package-details p::first-line{color:#07566a;font-weight:800}@media(max-width:700px){.managed-package-details{grid-template-columns:1fr}}</style></head>",
        )
        body = re.sub(
            rb"https://images\.unsplash\.com/photo-1579684385127-1ef15d508118[^\"']*",
            b"/assets/5364922d95c1661732fe35b928fdbc89.jpg",
            body,
        )
        body = body.replace(b"<h2>Laboratory Packages</h2>", b"<h2>Featured Tests</h2>")
        body = body.replace(
            b"onclick=\"showRibbonInfo('Test packages','Review grouped service packages.'); return false;\"",
            b"onclick=\"event.preventDefault();document.querySelector('#laboratory-packages .package-options').style.display='grid';document.querySelector('#laboratory-packages .lab-groups').style.display='none';document.querySelector('#laboratory-packages').style.display='block';document.querySelector('#laboratory-packages').scrollIntoView({behavior:'smooth'});return false;\"",
        )
        body = body.replace(
            b"onclick=\"showRibbonInfo('Featured test','See highlighted tests.'); return false;\"",
            b"onclick=\"event.preventDefault();document.querySelector('#laboratory-packages .package-options').style.setProperty('display','none','important');document.querySelector('#laboratory-packages .lab-groups').style.setProperty('display','grid','important');document.querySelector('#laboratory-packages').style.display='block';document.querySelector('#laboratory-packages').scrollIntoView({behavior:'smooth'});return false;\"",
        )
        body = body.replace(
            b"Choose from available laboratory examinations for your appointment request.",
            b"Choose from available laboratory examinations for your appointment request.</p><section class=\"package-picker\"><h3>What package are you looking for?</h3><div class=\"package-options\"><button type=\"button\">Health Test<br>Packages</button><button type=\"button\">Pre-employment<br>Packages</button><button type=\"button\">Specific Health<br>Packages</button><div class=\"managed-package-details\"><strong>Specific Health Packages</strong><p><b>Prenatal Package:</b> CBC with Platelet Count; Complete Urinalysis; HBsAg (RAPID) Screening; RPR; Anti-HBs (CMIA); HIV 1&amp;2 (Quali); Blood &amp; Rh Typing; Rubella IgG (CMIA); Fasting Blood Sugar</p><p><b>Dengue Screening Package:</b> CBC with Platelet Count; Dengue NS1; Dengue IgG/IgM</p><p><b>Screening Test for Anemia:</b> CBC with Platelet Count; Reticulocyte Count; TIBC + Fe; Ferritin; Peripheral Blood Smear; Optional: Vitamin B12; Hemoglobin Electrophoresis; Von Wilbrand Activity</p></div><button type=\"button\">IHC Panel<br>Packages</button><button type=\"button\">Autoimmune<br>Packages</button></div></section><p style=\"display:none\">",
        )
        body = re.sub(
            rb'<section class="package-picker">.*?</section><p style="display:none">',
            package_picker_markup.encode(),
            body,
            count=1,
            flags=re.DOTALL,
        )
        body = body.replace(
            b"Urinalysis, Fecalysis, Pregnancy Test, Occult Blood, Urine Ketone, Semanalysis",
            b"Urinalysis<br>Fecalysis<br>Pregnancy Test<br>Occult Blood<br>Urine Ketone<br>Semanalysis",
        )
        for source, formatted in {
            b"CBC, Platelet Count, Hgb/Hct, Differential Count, Clotting Time, Bleeding Time, ESR, Toxic Granules": b"CBC<br>Platelet Count<br>Hgb/Hct<br>Differential Count<br>Clotting Time<br>Bleeding Time<br>ESR<br>Toxic Granules",
            b"FBS, BUN, CREA, BUA, Cholesterol, Triglycerides, HDL, LDL, SGPT/ALT, SGOT/AST, OGTT, Sodium, Potassium, Chloride, HGT": b"FBS<br>BUN<br>CREA<br>BUA<br>Cholesterol<br>Triglycerides<br>HDL<br>LDL<br>SGPT/ALT<br>SGOT/AST<br>OGTT<br>Sodium<br>Potassium<br>Chloride<br>HGT",
            b"HBsAg, Anti-HCV, Anti-HAV IgM, HIV Test, Syphilis ICT/RPR, Dengue, Typhoid, HbA1c, Troponin, CK-MB, PSA, TSH": b"HBsAg<br>Anti-HCV<br>Anti-HAV IgM<br>HIV Test<br>Syphilis ICT/RPR<br>Dengue<br>Typhoid<br>HbA1c<br>Troponin<br>CK-MB<br>PSA<br>TSH",
            b"Blood Typing, RH Typing, Cross Matching, Newborn Screening, Other Tests": b"Blood Typing<br>RH Typing<br>Cross Matching<br>Newborn Screening<br>Other Tests",
        }.items():
            body = body.replace(source, formatted)
        body = body.replace(
            b"<style>#laboratory-packages .facility-copy{background:#f7faf9!important}#laboratory-packages .package-picker{margin:26px 0 34px;text-align:center}#laboratory-packages .package-picker h3{margin:0 0 18px;color:#07566a;font-size:22px}#laboratory-packages .package-options{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px}#laboratory-packages .package-options button,#laboratory-packages .region-options button{min-height:62px;border:0;border-radius:7px;background:#13a8a5;color:#fff;font:700 12px 'Trebuchet MS',Arial,sans-serif;cursor:pointer;box-shadow:0 6px 12px rgba(8,80,92,.12)}#laboratory-packages .package-options button:nth-child(2){background:#6d63a7}#laboratory-packages .package-options button:nth-child(3){background:#c44d7d}#laboratory-packages .package-options button:nth-child(4){background:#e3a00a}#laboratory-packages .package-options button:nth-child(5){background:#8b5db2}#laboratory-packages .region-heading{margin-top:28px!important;margin-bottom:5px!important;color:#17a879!important;font-size:22px!important}.region-note{margin:0 0 14px;color:#526b72;font-size:12px}.region-options{display:flex;justify-content:center;gap:20px}.region-options button{min-width:190px;background:#07566a!important}.region-options button:nth-child(2){background:#13bdbb!important}.region-options span{font-size:20px;margin-left:8px}#laboratory-packages .lab-layout{display:block!important}#laboratory-packages .lab-photo{display:none!important}#laboratory-packages .lab-groups{display:grid!important;grid-template-columns:repeat(3,minmax(0,1fr))!important;grid-auto-rows:minmax(190px,auto)!important;align-items:start!important;gap:28px 54px!important;margin-top:24px}#laboratory-packages .lab-group{min-height:190px;padding:0 0 12px!important;background:transparent!important;border:0!important}#laboratory-packages .lab-group h3{display:inline-flex;min-width:165px;margin:0 0 14px!important;padding:11px 16px;border-radius:5px;color:#fff!important;background:#18506b;font-size:16px!important}#laboratory-packages .lab-group:nth-child(2) h3{background:#a45176}#laboratory-packages .lab-group:nth-child(3) h3{background:#e29b09}#laboratory-packages .lab-group:nth-child(4) h3{background:#2ba6bb}#laboratory-packages .lab-group:nth-child(5) h3{background:#17a879}@media(max-width:800px){#laboratory-packages .package-options{grid-template-columns:repeat(2,minmax(0,1fr))}#laboratory-packages .lab-groups{grid-template-columns:repeat(2,minmax(0,1fr))!important;grid-auto-rows:minmax(180px,auto)!important}}@media(max-width:520px){#laboratory-packages .package-options,.region-options{grid-template-columns:1fr;flex-direction:column}#laboratory-packages .lab-groups{grid-template-columns:1fr!important;grid-auto-rows:auto!important}}</style></head>",
            b"",
        )
        body = body.replace(
            b"</head>",
            b"<style>#laboratory-packages .facility-copy{background:#f7faf9!important;padding:30px!important}#laboratory-packages .package-picker{margin:26px 0 34px;text-align:center}#laboratory-packages .package-picker h3{margin:0 0 18px;color:#07566a;font-size:22px}#laboratory-packages .package-options{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px}#laboratory-packages .package-options button{min-height:70px;padding:10px;border:0;border-radius:8px;background:#0e9f9b;color:#fff;font:700 13px 'Trebuchet MS',Arial,sans-serif;line-height:1.25;cursor:pointer;box-shadow:0 6px 14px rgba(8,80,92,.14)}#laboratory-packages .package-options button:nth-child(2){background:#7162a8}#laboratory-packages .package-options button:nth-child(3){background:#c64f7d}#laboratory-packages .package-options button:nth-child(4){background:#e19a0a}#laboratory-packages .package-options button:nth-child(5){background:#8b5db2}#laboratory-packages .lab-layout{display:block!important}#laboratory-packages .lab-photo{display:none!important}#laboratory-packages .lab-groups{display:grid!important;grid-template-columns:repeat(3,minmax(0,1fr))!important;grid-auto-rows:minmax(190px,auto)!important;align-items:start!important;gap:28px!important;margin-top:24px}#laboratory-packages .lab-group{min-height:190px;padding:0 0 14px!important;background:#fff!important;border:1px solid #dcebed!important;border-radius:12px!important;box-shadow:0 8px 18px rgba(8,26,34,.05);overflow:hidden}#laboratory-packages .lab-group h3{display:block;margin:0 0 14px!important;padding:12px 16px;color:#fff!important;background:#18506b;font-size:16px!important}#laboratory-packages .lab-group:nth-child(2) h3{background:#a45176}#laboratory-packages .lab-group:nth-child(3) h3{background:#e29b09}#laboratory-packages .lab-group:nth-child(4) h3{background:#2ba6bb}#laboratory-packages .lab-group:nth-child(5) h3{background:#17a879}#laboratory-packages .lab-group p{margin:0;padding:0 16px;color:#334b53!important;font-size:14px!important;line-height:1.9!important}@media(max-width:800px){#laboratory-packages .package-options{grid-template-columns:repeat(2,minmax(0,1fr))}#laboratory-packages .lab-groups{grid-template-columns:repeat(2,minmax(0,1fr))!important}}@media(max-width:520px){#laboratory-packages .package-options,#laboratory-packages .lab-groups{grid-template-columns:1fr!important}}</style></head>",
        )
        body = body.replace(b"</head>", b"<style>#laboratory-packages .region-heading,#laboratory-packages .region-note,#laboratory-packages .region-options,#laboratory-packages .lab-groups,#laboratory-packages .managed-package-details{display:none!important}</style></head>")
        body = body.replace(
            b"https://images.unsplash.com/photo-1587351021759-3e566b6af7cc?auto=format&amp;fit=crop&amp;w=900&amp;q=82",
            b"/assets/6ff75d9ad8fecce99b463aa69ba4fcf4.jpg",
        )
        body = body.replace(
            b"/assets/800436033_1604320875067949_4721912843219203405_n%20(1).jpg",
            b"/assets/800436033_1604320875067949_4721912843219203405_n.jpg",
        )
        headers = dict(response.headers)
        headers.pop("content-length", None)
        return Response(content=body, status_code=response.status_code, headers=headers, media_type="text/html")
    return response


app.add_middleware(GZipMiddleware, minimum_size=1024)


CLINICAL_SERVICES = [
    "Emergency & Urgent Care Assessment",
    "Prenatal Care Consultation (Maternal Check-up)",
    "Postnatal Care & Newborn Follow-up Visit",
    "Pediatric Care Consultation",
    "Dehydration & Diarrhea Management Protocol",
    "Outpatient General Medicine Clinic Visit",
    "Minor Surgical Procedure (Excision, Suturing, Etc.)",
    "Dental Services (Oral Exam, Extraction, Cleaning)",
    "Laboratory Examination (Complete Blood Panel)",
    "Urinalysis, Fecalysis & Routine Swabbing",
]

DIAGNOSTIC_IMAGING = [
    "OB-GYN Ultrasound (Pelvic / Transvaginal)",
    "X-Ray (Chest/Thorax Digital)",
    "Echocardiography (2D Echo With Doppler)",
    "Ultrasound (Whole Abdomen/Pelvic)",
    "ECG (12-Lead Cardiac Test)",
    "Digital Mammography (Breast Screening)",
    "CT Scan Plain / With Contrast Monitoring",
]

FACILITIES = [
    "GAPCC Main Diagnostic & Urgent Care Center",
    "GAPCC Satellite Outpatient Clinic",
    "GAPCC Dental & Pediatric Specialized Wing",
]

GENDER_OPTIONS = ["Female", "Male", "Non-binary", "Prefer not to say"]
DOCTOR_SPECIALIZATIONS = [
    "Allergy and Immunology",
    "Anesthesiology",
    "Cardiology",
    "Cardiothoracic Surgery",
    "Clinical Pathology",
    "Critical Care Medicine",
    "Dermatology",
    "Emergency Medicine",
    "Endocrinology",
    "Family Medicine",
    "Gastroenterology",
    "General Physician",
    "General Surgery",
    "Geriatrics",
    "Hematology",
    "Infectious Disease",
    "Internal Medicine",
    "Medical Oncology",
    "Nephrology",
    "Neurology",
    "Neurosurgery",
    "Nuclear Medicine",
    "Obstetrics and Gynecology",
    "Occupational Medicine",
    "Ophthalmology",
    "Orthopedic Surgery",
    "Otolaryngology (ENT)",
    "Pediatrics",
    "Physical Medicine and Rehabilitation",
    "Plastic Surgery",
    "Psychiatry",
    "Pulmonology",
    "Radiation Oncology",
    "Radiology",
    "Rheumatology",
    "Urology",
]
PASSWORD_POLICY_PATTERN = r"(?=.*[A-Z])(?=.*[a-z])(?=.*[0-9])(?=.*[^A-Za-z0-9]).{12,}"
PASSWORD_POLICY_MESSAGE = "Use at least 12 characters with uppercase and lowercase letters, numbers, and a special character. Longer passwords are more secure."

PORTAL_PROFILE_CSS = """
.portal-profile { display:grid; grid-template-columns:52px minmax(0,1fr); gap:16px; align-items:start; width:min(1200px,100%); margin:0 auto 20px; padding:20px 22px; background:#fff; border:1px solid #dcebed; border-radius:8px; color:#15333b; box-shadow:0 8px 20px rgba(27,76,84,.06); }
.portal-profile .profile-avatar { display:grid; place-items:center; width:48px; aspect-ratio:1; border-radius:50%; background:#e3f5f1; color:#087e7a; font:800 20px 'Trebuchet MS',Arial,sans-serif; }
.portal-profile .profile-main { min-width:0; }
.portal-profile .profile-heading { display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:8px 14px; }
.portal-profile .profile-label { margin:0; color:#0d8f83; font:800 10px 'Trebuchet MS',Arial,sans-serif; text-transform:uppercase; }
.portal-profile h2 { margin:4px 0 14px; color:#123f49; font:700 20px 'Trebuchet MS',Arial,sans-serif; }
.portal-profile .profile-badge { padding:6px 9px; border-radius:999px; background:#e5f8ef; color:#08794f; font:800 10px 'Trebuchet MS',Arial,sans-serif; }
.portal-profile .profile-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px 18px; }
.portal-profile .profile-field { display:grid; gap:4px; min-width:0; }
.portal-profile .profile-field span { color:#789097; font:700 10px 'Trebuchet MS',Arial,sans-serif; text-transform:uppercase; }
.portal-profile .profile-field strong { overflow-wrap:anywhere; color:#284c58; font:700 13px 'Trebuchet MS',Arial,sans-serif; }
.dashboard-counts { display:flex; align-items:center; gap:8px; width:max-content; max-width:100%; margin:0; padding:0; overflow:visible; border:0; background:transparent; box-shadow:none; }
.dashboard-count-link { position:relative; display:grid; place-items:center; width:38px; height:38px; border:1px solid #A7EBF2; border-radius:50%; background:#fff; color:#023859; text-decoration:none; }
.dashboard-count-link:hover, .dashboard-count-link:focus-visible { outline:2px solid #54ACBF; outline-offset:2px; background:#e8fafd; }
.dashboard-count-icon { font:700 17px 'Trebuchet MS',Arial,sans-serif; line-height:1; }
.dashboard-count-badge { position:absolute; top:-5px; right:-5px; display:grid; place-items:center; min-width:17px; height:17px; padding:0 4px; border:2px solid #fff; border-radius:999px; background:#c6374d; color:#fff; font:800 9px 'Trebuchet MS',Arial,sans-serif; }
.dashboard-count-link.alert-count .dashboard-count-badge { background:#c6374d; }
.dashboard-count-link.message-count .dashboard-count-badge { background:#26658C; }
.dashboard-count-link.booking-count .dashboard-count-badge { background:#54ACBF; color:#011C40; }
.dashboard-count-link.schedule-count .dashboard-count-badge { background:#023859; }
.sr-only { position:absolute; width:1px; height:1px; padding:0; margin:-1px; overflow:hidden; clip:rect(0,0,0,0); white-space:nowrap; border:0; }
@media(max-width:520px) { .portal-profile { grid-template-columns:40px minmax(0,1fr); gap:12px; padding:16px; } .portal-profile .profile-avatar { width:38px; font-size:16px; } .portal-profile .profile-grid { grid-template-columns:1fr; } }
"""

DOCTOR_DASHBOARD_CSS = """
.doctor-dashboard { display:grid; grid-template-columns:238px minmax(0,1fr); gap:20px; width:min(1580px,100%); margin:0 auto; color:#183c43; font-family:'Trebuchet MS',Arial,sans-serif; }
.doctor-sidebar { position:sticky; top:18px; align-self:start; display:flex; flex-direction:column; min-height:calc(100vh - 36px); padding:18px 14px; background:#fff; border:1px solid #dcebed; border-radius:8px; }
.doctor-brand { display:flex; align-items:center; gap:10px; padding:4px 8px 18px; border-bottom:1px solid #e4eeee; color:#164f58; text-decoration:none; font-size:16px; font-weight:800; }
.doctor-brand-mark { display:grid; place-items:center; width:30px; aspect-ratio:1; border-radius:8px; background:#0e8f82; color:#fff; font-size:16px; }
.doctor-sidebar-profile { display:grid; justify-items:center; gap:5px; padding:20px 8px 18px; text-align:center; }
.doctor-sidebar-profile .profile-avatar { display:grid; place-items:center; width:58px; aspect-ratio:1; border:3px solid #e1f2ed; border-radius:50%; background:#d8efe8; color:#087e7a; font-size:21px; font-weight:800; }
.doctor-sidebar-profile strong { color:#183c43; font-size:13px; }
.doctor-sidebar-profile small { color:#738a8d; font-size:10px; }
.doctor-nav { display:grid; grid-template-columns:minmax(0,1fr); gap:4px; width:100%; margin:0; padding:0; overflow:visible; border:0; background:transparent; box-shadow:none; }
.doctor-nav a, .doctor-home-link { display:flex; align-items:center; min-height:40px; padding:9px 10px; border-radius:5px; color:#516d72; font-size:12px; font-weight:700; text-decoration:none; }
.doctor-nav a:hover, .doctor-nav a:focus-visible { outline:none; background:#e9f6f3; color:#087e7a; }
.doctor-home-link { margin-top:auto; border-top:1px solid #e4eeee; border-radius:0; padding-top:16px; }
.doctor-dashboard-main { display:grid; align-content:start; gap:16px; min-width:0; }
.doctor-topbar { display:flex; align-items:center; justify-content:space-between; gap:16px; min-height:52px; }
.doctor-topbar p { margin:0; color:#71888c; font-size:11px; }
.doctor-topbar strong { color:#254b51; font-size:12px; }
.doctor-welcome { display:flex; align-items:flex-end; justify-content:space-between; gap:20px; min-height:162px; padding:24px 28px; overflow:hidden; border-radius:8px; background:linear-gradient(105deg,rgba(4,73,78,.94),rgba(13,133,119,.82)),url('/assets/portal.jpg') center 42%/cover; color:#fff; }
.doctor-welcome .eyebrow { margin:0 0 8px; color:#a7e1c7; font-size:10px; font-weight:800; text-transform:uppercase; }
.doctor-welcome h1 { margin:0; color:#fff; font-size:26px; }
.doctor-welcome p { margin:8px 0 0; color:rgba(255,255,255,.85); font-size:12px; }
.doctor-welcome .welcome-schedule { align-self:flex-end; max-width:260px; padding:12px 14px; border:1px solid rgba(255,255,255,.24); border-radius:6px; background:rgba(255,255,255,.1); color:#fff; font-size:11px; }
.doctor-stats { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; }
.doctor-stat { display:flex; align-items:center; gap:12px; min-height:84px; padding:16px; background:#fff; border:1px solid #dcebed; border-radius:8px; }
.doctor-stat .stat-icon { display:grid; place-items:center; width:38px; aspect-ratio:1; border-radius:50%; background:#e2f4ec; color:#078b72; font-size:15px; font-weight:800; }
.doctor-stat strong { display:block; color:#20484f; font-size:21px; }
.doctor-stat span { display:block; margin-top:2px; color:#7b9195; font-size:10px; }
.doctor-panels { display:grid; grid-template-columns:minmax(0,2fr) minmax(260px,.9fr); gap:14px; align-items:start; }
.doctor-panels #doctor-appointments { grid-column:1 / -1; }
.doctor-panel { min-width:0; padding:18px; background:#fff; border:1px solid #dcebed; border-radius:8px; }
.doctor-panel h2 { margin:0; color:#244b52; font-size:15px; }
.doctor-panel .panel-heading { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:14px; }
.doctor-panel .panel-heading small { color:#82979b; font-size:10px; }
.doctor-panel .table-scroll { margin:0; }
.doctor-panel table { min-width:570px; }
.doctor-profile-panel { display:grid; gap:12px; }
.doctor-profile-panel .profile-main { min-width:0; }
.doctor-profile-panel .profile-grid { grid-template-columns:1fr; }
.doctor-profile-panel .profile-field { padding-bottom:8px; border-bottom:1px solid #edf2f2; }
.doctor-forms { display:grid; grid-template-columns:minmax(0,.85fr) minmax(0,1.15fr); gap:14px; align-items:start; }
.doctor-forms #doctor-records, .doctor-forms #doctor-residents, .doctor-forms #doctor-results { grid-column:1 / -1; }
.doctor-panel form { margin:0; }
.doctor-panel label { margin-top:12px; color:#537074; font-size:10px; }
.doctor-panel input, .doctor-panel select, .doctor-panel textarea { min-width:0; border-color:#d6e4e3; background:#fbfdfc; color:#23464c; font:12px 'Trebuchet MS',Arial,sans-serif; }
.doctor-panel .button { width:100%; margin-top:14px; background:#087e7a; font-family:'Trebuchet MS',Arial,sans-serif; }
.doctor-panel .button.green { background:#178d70; }
.doctor-panel .doctor-search { display:flex; gap:8px; margin:0 0 14px; }
.doctor-panel .doctor-search input { flex:1; }
.doctor-panel .doctor-search .button { width:auto; min-width:100px; margin:0; }
.doctor-panel .doctor-patient-filters { display:grid; grid-template-columns:auto repeat(2,minmax(0,220px)); gap:10px; align-items:center; margin:0 0 14px; }
.doctor-panel .doctor-patient-filters strong { color:#153f49; font-size:12px; }
.doctor-panel .doctor-patient-filters label { display:grid; gap:5px; margin:0; color:#537074; font-size:10px; }
.doctor-panel .doctor-patient-filters select { width:100%; padding:10px 12px; }
.doctor-dashboard [id] { scroll-margin-top:16px; }
@media(max-width:1050px) { .doctor-dashboard { grid-template-columns:1fr; } .doctor-sidebar { position:static; min-height:0; } .doctor-sidebar-profile { display:flex; justify-content:flex-start; text-align:left; padding:14px 8px; } .doctor-sidebar-profile .profile-avatar { width:42px; } .doctor-nav { display:flex; flex-wrap:wrap; } .doctor-nav a { border:1px solid #e2ecec; } .doctor-home-link { margin-top:10px; } }
@media(max-width:720px) { .doctor-stats, .doctor-panels, .doctor-forms, .doctor-panel .doctor-patient-filters { grid-template-columns:1fr; } .doctor-welcome { align-items:flex-start; flex-direction:column; } .doctor-welcome .welcome-schedule { align-self:stretch; max-width:none; } .doctor-topbar { align-items:flex-start; flex-direction:column; } }
.doctor-dashboard { --palette-aqua:#A7EBF2; --palette-cyan:#54ACBF; --palette-mid:#26658C; --palette-deep:#023859; --palette-navy:#011C40; color:var(--palette-navy); background:#f3f9fb; }
.doctor-sidebar { border-color:var(--palette-cyan); background:linear-gradient(180deg,#fff 0%,#effafd 100%); }
.doctor-brand { border-bottom-color:var(--palette-aqua); color:var(--palette-navy); }
.doctor-brand-mark { background:var(--palette-mid); }
.doctor-sidebar-profile .profile-avatar { border-color:var(--palette-cyan); background:var(--palette-aqua); color:var(--palette-navy); }
.doctor-sidebar-profile strong, .doctor-topbar strong { color:var(--palette-navy); }
.doctor-sidebar-profile small, .doctor-topbar p { color:var(--palette-mid); }
.doctor-nav a, .doctor-home-link { color:var(--palette-deep); }
.doctor-nav a:hover, .doctor-nav a:focus-visible { background:var(--palette-aqua); color:var(--palette-navy); }
.doctor-home-link { border-top-color:var(--palette-aqua); }
.doctor-welcome { background:linear-gradient(105deg,rgba(1,28,64,.95),rgba(38,101,140,.84)),url('/assets/portal.jpg') center 42%/cover; }
.doctor-welcome .eyebrow { color:var(--palette-aqua); }
.doctor-welcome .welcome-schedule { border-color:rgba(167,235,242,.4); background:rgba(84,172,191,.16); }
.doctor-stat { border-color:var(--palette-aqua); }
.doctor-stat .stat-icon { background:var(--palette-aqua); color:var(--palette-deep); }
.doctor-stat strong, .doctor-panel h2 { color:var(--palette-navy); }
.doctor-panel { border-color:var(--palette-aqua); }
.doctor-panel .panel-heading small { color:var(--palette-mid); }
.doctor-profile-panel .profile-field { border-bottom-color:var(--palette-aqua); }
.doctor-panel input:focus-visible, .doctor-panel select:focus-visible, .doctor-panel textarea:focus-visible { border-color:var(--palette-cyan); outline:2px solid rgba(84,172,191,.28); }
.doctor-panel .button { background:var(--palette-deep); }
.doctor-panel .button.green { background:var(--palette-mid); }
.doctor-sidebar > .portal-profile { width:100%; grid-template-columns:minmax(0,1fr); justify-items:center; gap:10px; margin:14px 0; padding:14px 10px; border-color:var(--palette-aqua); background:#f3f9fb; box-shadow:none; }
.doctor-sidebar > .portal-profile .profile-avatar { width:50px; border:3px solid var(--palette-cyan); background:var(--palette-aqua); color:var(--palette-navy); }
.doctor-sidebar > .portal-profile .profile-main { width:100%; }
.doctor-sidebar > .portal-profile .profile-heading { justify-content:center; }
.doctor-sidebar > .portal-profile .profile-label { color:var(--palette-mid); text-align:center; }
.doctor-sidebar > .portal-profile .profile-badge { background:var(--palette-aqua); color:var(--palette-deep); }
.doctor-sidebar > .portal-profile h2 { margin-bottom:12px; color:var(--palette-navy); font-size:15px; text-align:center; }
.doctor-sidebar > .portal-profile .profile-grid { grid-template-columns:minmax(0,1fr); gap:8px; }
.doctor-sidebar > .portal-profile .profile-field { padding-bottom:7px; border-bottom:1px solid var(--palette-aqua); }
.doctor-sidebar > .portal-profile .profile-field span { color:var(--palette-mid); }
.doctor-sidebar > .portal-profile .profile-field strong { color:var(--palette-deep); font-size:11px; }
@media(max-width:1050px) { .doctor-sidebar > .portal-profile { grid-template-columns:42px minmax(0,1fr); justify-items:stretch; gap:12px; } .doctor-sidebar > .portal-profile .profile-avatar { width:42px; } .doctor-sidebar > .portal-profile .profile-heading { justify-content:space-between; } .doctor-sidebar > .portal-profile .profile-label { text-align:left; } .doctor-sidebar > .portal-profile h2 { text-align:left; } .doctor-sidebar > .portal-profile .profile-grid { grid-column:1 / -1; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); } }
"""

DOCTOR_PATIENT_FILTER_SCRIPT = """
document.addEventListener('DOMContentLoaded', function () {
    const directory = document.querySelector('[data-doctor-patient-directory]');
    const ageFilter = document.querySelector('[data-doctor-patient-age-filter]');
    const genderFilter = document.querySelector('[data-doctor-patient-gender-filter]');
    if (!directory || !ageFilter || !genderFilter) return;
    function filterPatients() {
        directory.querySelectorAll('tbody tr[data-age-group]').forEach(function (row) {
            const matchesAge = !ageFilter.value || row.dataset.ageGroup === ageFilter.value;
            const matchesGender = !genderFilter.value || row.dataset.gender === genderFilter.value;
            row.hidden = !matchesAge || !matchesGender;
        });
    }
    ageFilter.addEventListener('change', filterPatients);
    genderFilter.addEventListener('change', filterPatients);
});
"""

ADMIN_DASHBOARD_CSS = """
.admin-dashboard { display:grid; grid-template-columns:256px minmax(0,1fr); align-items:start; gap:20px; max-width:1680px; margin:-8px auto 0; color:#15333b; }
.admin-dashboard .admin-sidebar { position:sticky; top:18px; max-height:calc(100vh - 36px); overflow:auto; padding:18px 12px; background:linear-gradient(180deg,#fff 0%,#f5fbfc 100%); border:1px solid #d9e8eb; border-radius:8px; box-shadow:0 8px 22px rgba(8,50,76,.08); }
.admin-dashboard .admin-sidebar-brand { display:grid; gap:3px; padding:4px 10px 16px; border-bottom:1px solid #d9e8eb; }
.admin-dashboard .admin-sidebar-brand span { color:#0e7d87; font-family:'Trebuchet MS',Arial,sans-serif; font-size:11px; font-weight:800; letter-spacing:.1em; text-transform:uppercase; }
.admin-dashboard .admin-sidebar-brand strong { color:#07566a; font-family:'Trebuchet MS',Arial,sans-serif; font-size:18px; }
.admin-dashboard .admin-sidebar .portal-profile { width:100%; grid-template-columns:34px minmax(0,1fr); gap:9px; margin:14px 0 6px; padding:12px 8px; border-radius:7px; box-shadow:none; }
.admin-dashboard .admin-sidebar .portal-profile .profile-avatar { width:32px; font-size:14px; }
.admin-dashboard .admin-sidebar .portal-profile .profile-heading { display:block; }
.admin-dashboard .admin-sidebar .portal-profile .profile-label { font-size:9px; }
.admin-dashboard .admin-sidebar .portal-profile .profile-badge { display:inline-flex; margin-top:5px; }
.admin-dashboard .admin-sidebar .portal-profile h2 { margin:6px 0 10px; font-size:14px; }
.admin-dashboard .admin-sidebar .portal-profile .profile-grid { grid-template-columns:1fr; gap:8px; }
.admin-dashboard .admin-sidebar .portal-profile .profile-field strong { font-size:11px; }
.admin-dashboard nav.admin-sidebar-nav { display:grid; grid-template-columns:minmax(0,1fr); align-content:start; gap:4px; width:100%; margin:0; padding:12px 0 0; overflow:visible; background:transparent; border:0; box-shadow:none; font-family:'Trebuchet MS',Arial,sans-serif; }
.admin-dashboard .admin-sidebar-group { margin:12px 10px 4px; color:#42636b; font-family:'Trebuchet MS',Arial,sans-serif; font-size:10px; font-weight:800; letter-spacing:.08em; text-transform:uppercase; }
.admin-dashboard nav.admin-sidebar-nav a { display:flex; align-items:center; justify-content:flex-start; min-width:0; min-height:42px; padding:9px 11px; border-left:3px solid transparent; border-radius:4px; color:#284c58; font-family:'Trebuchet MS',Arial,sans-serif; font-size:13px; font-weight:700; line-height:1.3; text-align:left; text-decoration:none; white-space:normal; }
.admin-dashboard nav.admin-sidebar-nav a:hover { background:#eaf9fb; color:#07566a; }
.admin-dashboard nav.admin-sidebar-nav a:focus-visible { outline:2px solid #12c7d1; outline-offset:2px; }
.admin-dashboard nav.admin-sidebar-nav a.is-active { border-left-color:#078b83; background:#e5f7f8; color:#053d4f; }
.admin-dashboard .admin-content { min-width:0; }
.admin-dashboard .admin-content > .dashboard-counts { margin:0 0 -6px auto; }
.admin-dashboard [data-admin-section] { scroll-margin-top:18px; }
.admin-dashboard, .admin-dashboard button, .admin-dashboard input { font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
.admin-dashboard .admin-hero { display:flex; align-items:flex-end; justify-content:space-between; gap:24px; padding:28px 4px 24px; }
.admin-dashboard .eyebrow { margin:0 0 8px; color:#0d8f83; font-size:11px; font-weight:800; letter-spacing:.14em; text-transform:uppercase; }
.admin-dashboard .admin-hero h1 { margin:0; color:#123f49; font-size:clamp(2rem, 4vw, 3.25rem); letter-spacing:-.045em; }
.admin-dashboard .admin-hero p { margin:10px 0 0; color:#6b8187; font-size:14px; }
.admin-dashboard .metric-focus { display:flex; align-items:center; gap:18px; min-width:285px; padding:18px 22px; background:#fff; border:1px solid #d8ebed; border-radius:12px; box-shadow:0 10px 26px rgba(22,73,82,.08); }
.admin-dashboard .metric-focus .metric-value { color:#07566a; font-size:42px; font-weight:850; line-height:1; }
.admin-dashboard .metric-focus strong { display:block; color:#1a4852; font-size:13px; }
.admin-dashboard .metric-focus small { display:block; margin-top:5px; color:#789097; font-size:11px; }
.admin-dashboard .analytics-grid { display:grid; grid-template-columns:1fr 1.45fr 1fr; gap:16px; margin-bottom:20px; }
.admin-dashboard .dash-card { min-height:176px; padding:20px; background:#fff; border:1px solid #dcebed; border-radius:12px; box-shadow:0 10px 24px rgba(27,76,84,.07); }
.admin-dashboard .dash-card h2 { margin:0; color:#486a71; font-size:13px; font-weight:750; letter-spacing:.01em; }
.admin-dashboard .card-top { display:flex; align-items:center; justify-content:space-between; gap:10px; }
.admin-dashboard .big-number { display:block; margin-top:18px; color:#123f49; font-size:45px; font-weight:850; letter-spacing:-.05em; }
.admin-dashboard .subtle { color:#7a9096; font-size:12px; }
.admin-dashboard .active-badge, .admin-dashboard .trend-tag, .admin-dashboard .status-badge, .admin-dashboard .doctor-tag { display:inline-flex; align-items:center; gap:6px; width:max-content; padding:6px 9px; border-radius:999px; font-size:10px; font-weight:800; }
.admin-dashboard .active-badge { background:#e5f8ef; color:#08794f; }
.admin-dashboard .active-badge::before { width:6px; height:6px; border-radius:50%; background:#13b978; content:""; box-shadow:0 0 0 3px #c8f0de; }
.admin-dashboard .trend-tag { background:#e6f6f5; color:#0a817a; }
.admin-dashboard .chart { display:flex; align-items:flex-end; justify-content:space-around; height:102px; margin-top:13px; padding:0 8px; border-bottom:1px solid #dcebed; }
.admin-dashboard .bar-wrap { display:flex; flex-direction:column; align-items:center; justify-content:flex-end; height:100%; gap:7px; color:#789097; font-size:10px; }
.admin-dashboard .bar { width:24px; min-height:10px; border-radius:5px 5px 0 0; background:linear-gradient(180deg,#28c4ad,#0b8b87); box-shadow:0 5px 10px rgba(8,145,132,.16); }
.admin-dashboard .bar.muted { background:#b8e7e0; box-shadow:none; }
.admin-dashboard .database-search { display:flex; align-items:flex-end; justify-content:space-between; gap:18px; margin:0 0 20px; padding:20px; background:#eaf8f6; border:1px solid #c7e9e5; border-radius:12px; }
.admin-dashboard .database-search label { margin:0 0 7px; color:#0a706d; }
.admin-dashboard .database-search .search-field { flex:1; }
.admin-dashboard .database-search input { height:46px; border-color:#b9deda; background:#fff; border-radius:7px; }
.admin-dashboard .database-search button { height:46px; margin:0; padding:0 20px; border:0; border-radius:7px; background:#087e7a; color:#fff; font-size:12px; font-weight:800; cursor:pointer; }
.admin-dashboard .table-card { margin-bottom:20px; padding:20px 20px 6px; background:#fff; border:1px solid #dcebed; border-radius:12px; box-shadow:0 10px 24px rgba(27,76,84,.07); }
.admin-dashboard .table-heading { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:14px; }
.admin-dashboard .table-heading h2 { margin:0; color:#153f49; font-size:16px; }
.admin-dashboard .table-heading span { color:#82979c; font-size:11px; }
.admin-dashboard .patient-filters { display:grid; grid-template-columns:auto repeat(2,minmax(0,220px)); gap:10px; align-items:center; margin:0 0 14px; }
.admin-dashboard .patient-filters strong { color:#153f49; font-size:12px; }
.admin-dashboard .patient-filters label { display:grid; gap:5px; margin:0; color:#688188; font-size:10px; }
.admin-dashboard .patient-filters select { width:100%; min-width:0; padding:10px 12px; border:1px solid #d3e5e4; border-radius:6px; background:#fff; color:#35545b; font-size:12px; }
.admin-dashboard .table-scroll { margin:0; border:0; border-radius:0; }
.admin-dashboard table { min-width:760px; box-shadow:none; }
.admin-dashboard th { padding:11px 12px; background:#f5faf9; color:#6b858a; border-bottom:1px solid #e3efef; font-size:10px; letter-spacing:.08em; }
.admin-dashboard td { padding:14px 12px; color:#35545b; border-bottom:1px solid #edf3f3; font-size:12px; }
.admin-dashboard tr:last-child td { border-bottom:0; }
.admin-dashboard .primary-cell { color:#154653; font-weight:800; }
.admin-dashboard .secondary-cell { display:block; margin-top:3px; color:#8aa0a4; font-size:11px; }
.admin-dashboard .status-badge.active { background:#e5f8ef; color:#08794f; }
.admin-dashboard .status-badge.pending { background:#fff4dc; color:#a46b16; }
.admin-dashboard .doctor-tag { background:#edf7fb; color:#18708b; }
.admin-dashboard .doctor-tag.unassigned { background:#fff0ec; color:#bd5d45; }
.admin-dashboard .delete-button { padding:7px 10px; border:1px solid #f0c9c1; border-radius:6px; background:#fff7f5; color:#b84d3d; font-size:10px; font-weight:800; cursor:pointer; }
.admin-dashboard .delete-button:hover { background:#b84d3d; color:#fff; }
.admin-dashboard .manage-form { display:grid; grid-template-columns:1.1fr 1fr 1fr auto; gap:8px; align-items:end; }
.admin-dashboard .medication-add-form { grid-template-columns:1.4fr .8fr 1fr .7fr .7fr 1.4fr auto; margin-bottom:16px; }
.admin-dashboard .stock-update-form { display:flex; align-items:center; gap:6px; }
.admin-dashboard .stock-update-form input { width:76px; padding:7px; font-size:11px; }
.admin-dashboard .stock-update-form button { padding:8px 10px; border:0; border-radius:6px; background:#087e7a; color:#fff; font-size:10px; font-weight:800; cursor:pointer; }
.admin-dashboard .stock-badge { display:inline-flex; padding:6px 9px; border-radius:999px; font-size:10px; font-weight:800; white-space:nowrap; }
.admin-dashboard .stock-badge.available { background:#e5f8ef; color:#08794f; }
.admin-dashboard .stock-badge.low { background:#fff4dc; color:#a46b16; }
.admin-dashboard .stock-badge.out-of-stock { background:#fff1e8; color:#a54c22; }
.admin-dashboard .restock-reminder { margin:12px 0 16px; padding:14px 16px; border:1px solid #efc48d; border-left:4px solid #d78321; border-radius:6px; background:#fff9ed; color:#7b4b12; font-size:12px; }
.admin-dashboard .restock-reminder strong { display:block; margin-bottom:6px; }
.admin-dashboard .restock-reminder ul { margin:0; padding-left:18px; }
.admin-dashboard .medication-controls { display:grid; grid-template-columns:minmax(0,1fr) 220px; gap:10px; margin:0 0 14px; }
.admin-dashboard .medication-controls input, .admin-dashboard .medication-controls select { min-width:0; padding:10px 12px; border:1px solid #d3e5e4; border-radius:6px; background:#fff; color:#35545b; font-size:12px; }
.admin-dashboard .edit-button { padding:7px 10px; border:1px solid #c8e3e0; border-radius:6px; background:#f4fbfa; color:#087e7a; font-size:10px; font-weight:800; cursor:pointer; }
.admin-dashboard .medication-edit-dialog { width:min(560px, calc(100vw - 32px)); max-height:calc(100dvh - 32px); margin:auto; padding:22px; overflow:auto; border:1px solid #d5e8e7; border-radius:8px; color:#15333b; box-shadow:0 20px 60px rgba(11,48,56,.24); }
.admin-dashboard .medication-edit-dialog::backdrop { background:rgba(12,38,43,.48); }
.admin-dashboard .medication-edit-dialog h3 { margin:0 0 16px; color:#153f49; font-size:18px; }
.admin-dashboard .medication-edit-form { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; }
.admin-dashboard .medication-edit-form label { display:grid; gap:6px; margin:0; color:#688188; font-size:10px; text-transform:none; }
.admin-dashboard .medication-edit-form input, .admin-dashboard .medication-edit-form textarea { width:100%; min-width:0; padding:10px; border:1px solid #d3e5e4; border-radius:6px; background:#fff; color:#35545b; font:inherit; font-size:12px; }
.admin-dashboard .medication-edit-form .wide, .admin-dashboard .medication-edit-form .dialog-actions { grid-column:1 / -1; }
.admin-dashboard .medication-edit-form .dialog-actions { display:flex; justify-content:flex-end; gap:8px; }
.admin-dashboard .medication-edit-form .dialog-actions button { padding:9px 12px; border:1px solid #c8e3e0; border-radius:6px; background:#f4fbfa; color:#087e7a; font-size:11px; font-weight:800; cursor:pointer; }
.admin-dashboard .medication-edit-form .dialog-actions button[type="submit"] { border-color:#087e7a; background:#087e7a; color:#fff; }
.admin-dashboard .manage-form label { margin:0; color:#688188; font-size:9px; }
.admin-dashboard .manage-form select, .admin-dashboard .manage-form input { padding:8px; font-size:11px; }
.admin-dashboard .day-field { display:grid; gap:4px; margin:0; }
.admin-dashboard .day-field > span { color:#688188; font-size:9px; text-transform:uppercase; letter-spacing:.04em; }
.admin-dashboard .day-dropdown summary { list-style:none; cursor:pointer; padding:8px 28px 8px 10px; border:1px solid #cfd8e3; border-radius:6px; background:#fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M1 1l4 4 4-4' fill='none' stroke='%23688188' stroke-width='1.5'/%3E%3C/svg%3E") no-repeat right 10px center; color:#16343a; font-size:11px; font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.admin-dashboard .day-dropdown summary::-webkit-details-marker { display:none; }
.admin-dashboard .day-dropdown[open] summary { border-color:#087e7a; box-shadow:0 0 0 2px rgba(8,126,122,.15); }
.admin-dashboard .operations-card .manage-form { grid-template-columns:1fr 1fr; align-items:start; }
.admin-dashboard .operations-card .manage-form button { grid-column:1 / -1; }
.admin-dashboard .operations-card .manage-form .publish-button { background:#fff; color:#087e7a; border:1px solid #087e7a; }
.admin-dashboard .operations-card .manage-form .publish-button:hover { background:#087e7a; color:#fff; }
.admin-dashboard .day-field { position:relative; }
.admin-dashboard .day-menu { position:absolute; z-index:30; top:100%; left:0; min-width:100%; margin-top:4px; padding:4px; border:1px solid #dfe7ee; border-radius:6px; background:#fff; box-shadow:0 6px 16px rgba(22,52,58,.1); }
.admin-dashboard .manage-form .day-menu label { display:flex; align-items:center; gap:8px; margin:0; padding:6px 8px; border-radius:4px; color:#16343a; font-size:11px; font-weight:500; text-transform:none; letter-spacing:0; cursor:pointer; }
.admin-dashboard .manage-form .day-menu label:hover { background:#eef6f6; }
.admin-dashboard .manage-form .day-menu input { width:14px; height:14px; margin:0; padding:0; accent-color:#087e7a; }
.admin-dashboard .manage-form button { padding:9px 12px; border:0; border-radius:6px; background:#087e7a; color:#fff; font-size:10px; font-weight:800; cursor:pointer; }
.admin-dashboard .operations-grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:20px; }
.admin-dashboard .operations-card { padding:18px 20px; background:#f8fcfb; border:1px solid #dcebed; border-radius:12px; }
.admin-dashboard .operations-card h2 { margin:0 0 14px; color:#153f49; font-size:15px; }
.admin-dashboard .operation-note { margin:10px 0 0; color:#789097; font-size:11px; }
.admin-dashboard .empty-row { padding:25px; text-align:center; color:#81969b; }
@media (max-width: 1050px) { .admin-dashboard { grid-template-columns:minmax(0,1fr); gap:12px; } .admin-dashboard .admin-sidebar { position:static; max-height:none; padding:12px 14px; } .admin-dashboard .admin-sidebar-brand { padding-bottom:10px; } .admin-dashboard nav.admin-sidebar-nav { display:grid; grid-template-columns:repeat(auto-fit,minmax(145px,1fr)); align-items:stretch; gap:6px; overflow:visible; padding:10px 0 2px; } .admin-dashboard .admin-sidebar-group { display:block; grid-column:1 / -1; margin:8px 4px 0; } .admin-dashboard nav.admin-sidebar-nav a { min-height:42px; padding:9px; border:1px solid #d9e8eb; border-left:3px solid transparent; background:#fff; white-space:normal; } .admin-dashboard nav.admin-sidebar-nav a.is-active { border-color:#b8dfe2; border-left-color:#078b83; background:#e5f7f8; } }
@media (max-width: 900px) { .admin-dashboard .admin-hero { align-items:stretch; flex-direction:column; } .admin-dashboard .metric-focus { min-width:0; } .admin-dashboard .analytics-grid { grid-template-columns:1fr 1fr; } .admin-dashboard .analytics-grid .dash-card:nth-child(2) { grid-column:span 2; } .admin-dashboard .medication-add-form { grid-template-columns:repeat(3,minmax(0,1fr)); } }
@media (max-width: 600px) { .admin-dashboard .analytics-grid { grid-template-columns:1fr; } .admin-dashboard .analytics-grid .dash-card:nth-child(2) { grid-column:auto; } .admin-dashboard .database-search { align-items:stretch; flex-direction:column; } .admin-dashboard .medication-add-form, .admin-dashboard .medication-controls, .admin-dashboard .patient-filters, .admin-dashboard .medication-edit-form { grid-template-columns:1fr; } .admin-dashboard .medication-edit-form .wide, .admin-dashboard .medication-edit-form .dialog-actions { grid-column:auto; } }
@media (max-width: 600px) { .admin-dashboard .operations-grid { grid-template-columns:minmax(0,1fr); } .admin-dashboard .operations-card { min-width:0; } }
.admin-dashboard { --palette-aqua:#A7EBF2; --palette-cyan:#54ACBF; --palette-mid:#26658C; --palette-deep:#023859; --palette-navy:#011C40; color:var(--palette-navy); }
.admin-dashboard .admin-sidebar { background:linear-gradient(180deg,#fff 0%,#effafd 100%); border-color:var(--palette-cyan); }
.admin-dashboard .admin-sidebar-brand { border-bottom-color:var(--palette-aqua); }
.admin-dashboard .admin-sidebar-brand span, .admin-dashboard .admin-sidebar-group { color:var(--palette-mid); }
.admin-dashboard .admin-sidebar-brand strong { color:var(--palette-navy); }
.admin-dashboard .admin-sidebar .portal-profile { border-color:var(--palette-aqua); background:#f5fcfd; }
.admin-dashboard .admin-sidebar .portal-profile .profile-avatar { background:var(--palette-aqua); color:var(--palette-navy); }
.admin-dashboard .admin-sidebar .portal-profile .profile-label { color:var(--palette-mid); }
.admin-dashboard .admin-sidebar .portal-profile .profile-badge { background:var(--palette-aqua); color:var(--palette-deep); }
.admin-dashboard nav.admin-sidebar-nav a { color:var(--palette-deep); }
.admin-dashboard nav.admin-sidebar-nav a:hover { background:var(--palette-aqua); color:var(--palette-navy); }
.admin-dashboard nav.admin-sidebar-nav a:focus-visible { outline-color:var(--palette-cyan); }
.admin-dashboard nav.admin-sidebar-nav a.is-active { border-left-color:var(--palette-mid); background:#dff7fa; color:var(--palette-navy); }
.admin-dashboard .eyebrow { color:var(--palette-mid); }
.admin-dashboard .admin-hero h1, .admin-dashboard .metric-focus .metric-value, .admin-dashboard .big-number { color:var(--palette-navy); }
.admin-dashboard .metric-focus { border-color:var(--palette-cyan); }
.admin-dashboard .analytics-grid .dash-card, .admin-dashboard .table-card { border-color:var(--palette-aqua); }
.admin-dashboard .dash-card h2, .admin-dashboard .table-heading h2 { color:var(--palette-deep); }
.admin-dashboard .bar { background:linear-gradient(180deg,var(--palette-cyan),var(--palette-deep)); box-shadow:0 5px 10px rgba(2,56,89,.16); }
.admin-dashboard .bar.muted { background:var(--palette-aqua); box-shadow:none; }
.admin-dashboard .database-search { border-color:var(--palette-cyan); background:#eaf9fb; }
.admin-dashboard .database-search label { color:var(--palette-deep); }
.admin-dashboard .database-search button, .admin-dashboard .manage-form button, .admin-dashboard .stock-update-form button { background:var(--palette-deep); }
.admin-dashboard th { background:#dff7fa; color:var(--palette-navy); }
.admin-dashboard .primary-cell { color:var(--palette-deep); }
.admin-dashboard .operations-card { border-color:var(--palette-aqua); background:#f4fbfc; }
.admin-dashboard .operations-card h2 { color:var(--palette-deep); }
.admin-dashboard .edit-button { border-color:var(--palette-cyan); color:var(--palette-deep); }
.admin-dashboard .account-request-actions { display:grid; grid-template-columns:minmax(130px,1fr) auto auto; gap:6px; align-items:center; min-width:330px; }
.admin-dashboard .account-request-actions input { min-width:0; padding:8px; border:1px solid var(--palette-cyan); border-radius:5px; font-size:11px; }
.admin-dashboard .account-request-actions button { padding:8px 10px; border:0; border-radius:5px; background:var(--palette-deep); color:#fff; font-size:10px; font-weight:800; cursor:pointer; }
.admin-dashboard .account-request-actions button.reject-request { border:1px solid var(--palette-cyan); background:#fff; color:var(--palette-deep); }
"""

ADMIN_DASHBOARD_SCRIPT = """
document.addEventListener('DOMContentLoaded', function () {
    function filterAdminTables() {
        const adminSearch = document.querySelector('[data-admin-search]');
        const ageFilter = document.querySelector('[data-patient-age-filter]');
        const genderFilter = document.querySelector('[data-patient-gender-filter]');
        const query = adminSearch ? adminSearch.value.trim().toLowerCase() : '';
        const age = ageFilter ? ageFilter.value : '';
        const gender = genderFilter ? genderFilter.value : '';
        document.querySelectorAll('[data-filter-table]').forEach(function (table) {
            const isPatientDirectory = table.hasAttribute('data-patient-directory');
            const selectedAge = isPatientDirectory ? age : '';
            const selectedGender = isPatientDirectory ? gender : '';
            table.querySelectorAll('tbody tr').forEach(function (row) {
                const matchesSearch = !query || row.textContent.toLowerCase().includes(query);
                const matchesAge = !selectedAge || row.dataset.ageGroup === selectedAge;
                const matchesGender = !selectedGender || row.dataset.gender === selectedGender;
                row.hidden = !matchesSearch || !matchesAge || !matchesGender;
            });
        });
    }
    function filterMedications() {
        const medicationSearch = document.querySelector('[data-medication-search]');
        const medicationFilter = document.querySelector('[data-medication-filter]');
        const query = medicationSearch ? medicationSearch.value.trim().toLowerCase() : '';
        const status = medicationFilter ? medicationFilter.value : '';
        document.querySelectorAll('[data-medication-table] tbody tr').forEach(function (row) {
            const matchesSearch = !query || row.textContent.toLowerCase().includes(query);
            const matchesStatus = !status || row.dataset.medicationStatus === status;
            row.hidden = !matchesSearch || !matchesStatus;
        });
    }
    window.gapccReapplyFilters = function () { filterAdminTables(); filterMedications(); };
    document.addEventListener('input', function (event) {
        if (event.target.closest('[data-admin-search]')) filterAdminTables();
        if (event.target.closest('[data-medication-search]')) filterMedications();
    });
    document.addEventListener('change', function (event) {
        if (event.target.closest('[data-patient-age-filter]')) filterAdminTables();
        if (event.target.closest('[data-patient-gender-filter]')) filterAdminTables();
        if (event.target.closest('[data-medication-filter]')) filterMedications();
    });
    document.addEventListener('click', function (event) {
        const open = event.target.closest('[data-open-medication-edit]');
        if (open) {
            const dialog = document.getElementById(open.dataset.openMedicationEdit);
            if (dialog && !dialog.open) dialog.showModal();
        }
        const close = event.target.closest('[data-close-medication-edit]');
        if (close) close.closest('dialog').close();
    });
});
"""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def token_hash(token):
    return sha256(token.encode("utf-8")).hexdigest()


def password_meets_policy(password):
    return (
        len(password) >= 12
        and any(character.isupper() for character in password)
        and any(character.islower() for character in password)
        and any(character.isdigit() for character in password)
        and any(not character.isalnum() for character in password)
    )


def connect_db():
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database():
    with connect_db() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS patients (
                patient_id TEXT PRIMARY KEY,
                last_name TEXT NOT NULL,
                given_name TEXT NOT NULL,
                middle_name TEXT NOT NULL DEFAULT '',
                gender TEXT NOT NULL DEFAULT 'Prefer not to say',
                date_of_birth TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL UNIQUE,
                contact_number TEXT NOT NULL,
                portal_token_hash TEXT NOT NULL,
                portal_activated INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bookings (
                booking_id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_id TEXT NOT NULL UNIQUE,
                patient_id TEXT NOT NULL REFERENCES patients(patient_id),
                category TEXT NOT NULL,
                service TEXT NOT NULL,
                facility TEXT NOT NULL,
                appointment_date TEXT NOT NULL,
                appointment_time TEXT NOT NULL DEFAULT 'Clinic hours: 8:00 AM - 3:00 PM',
                status TEXT NOT NULL DEFAULT 'Scheduled',
                confirmation_acknowledged_at TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS recovery_otps (
                otp_id TEXT PRIMARY KEY,
                patient_id TEXT NOT NULL REFERENCES patients(patient_id),
                code_hash TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                used INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS account_recovery_otps (
                otp_id TEXT PRIMARY KEY,
                account_type TEXT NOT NULL CHECK (account_type IN ('admin', 'doctor')),
                account_id TEXT NOT NULL,
                code_hash TEXT NOT NULL,
                expires_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                used INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS appointment_messages (
                message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                booking_id INTEGER NOT NULL REFERENCES bookings(booking_id),
                patient_id TEXT NOT NULL REFERENCES patients(patient_id),
                message_type TEXT NOT NULL,
                channel TEXT NOT NULL,
                destination TEXT NOT NULL,
                message_body TEXT NOT NULL,
                scheduled_for TEXT NOT NULL,
                sent_at TEXT,
                status TEXT NOT NULL DEFAULT 'Queued',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS smtp_settings (
                setting_key TEXT PRIMARY KEY,
                encrypted_value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS admins (
                admin_id TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS clinic_packages (
                package_id INTEGER PRIMARY KEY AUTOINCREMENT,
                package_key TEXT NOT NULL UNIQUE,
                package_name TEXT NOT NULL,
                package_details TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS clinic_medications (
                medication_id INTEGER PRIMARY KEY AUTOINCREMENT,
                medication_name TEXT NOT NULL,
                strength TEXT NOT NULL DEFAULT '',
                dosage_form TEXT NOT NULL DEFAULT '',
                quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
                unit TEXT NOT NULL DEFAULT 'units',
                notes TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS doctors (
                doctor_id TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                specialization TEXT NOT NULL,
                facility TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                doctor_role TEXT NOT NULL DEFAULT 'Attending Physician',
                registration_number TEXT NOT NULL DEFAULT '',
                residency_program TEXT NOT NULL DEFAULT '',
                residency_year TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS account_requests (
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_type TEXT NOT NULL CHECK (account_type IN ('admin', 'doctor')),
                display_name TEXT NOT NULL,
                email TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                specialization TEXT NOT NULL DEFAULT '',
                facility TEXT NOT NULL DEFAULT '',
                registration_number TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'Pending',
                created_at TEXT NOT NULL,
                reviewed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS doctor_schedules (
                schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                doctor_id TEXT NOT NULL REFERENCES doctors(doctor_id),
                day_of_week TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS patient_records (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id TEXT NOT NULL REFERENCES patients(patient_id),
                doctor_id TEXT REFERENCES doctors(doctor_id),
                booking_id INTEGER REFERENCES bookings(booking_id),
                record_type TEXT NOT NULL,
                note TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS lab_results (
                result_id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id TEXT NOT NULL REFERENCES patients(patient_id),
                doctor_id TEXT REFERENCES doctors(doctor_id),
                booking_id INTEGER REFERENCES bookings(booking_id),
                test_name TEXT NOT NULL,
                result_summary TEXT NOT NULL,
                result_status TEXT NOT NULL DEFAULT 'Released',
                collected_at TEXT NOT NULL,
                released_at TEXT,
                pdf_filename TEXT,
                pdf_data BLOB,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS lab_result_notifications (
                notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_id INTEGER NOT NULL UNIQUE REFERENCES lab_results(result_id),
                destination TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'Pending',
                scheduled_for TEXT NOT NULL,
                sent_at TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS result_access_otps (
                otp_id TEXT PRIMARY KEY,
                patient_id TEXT NOT NULL REFERENCES patients(patient_id),
                delivery_channel TEXT NOT NULL,
                destination TEXT NOT NULL,
                code_hash TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                used INTEGER NOT NULL DEFAULT 0,
                
                created_at TEXT NOT NULL
            );
            """
        )
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(patients)")}
        if "gender" not in columns:
            connection.execute(
                "ALTER TABLE patients ADD COLUMN gender TEXT NOT NULL DEFAULT 'Prefer not to say'"
            )
        if "date_of_birth" not in columns:
            connection.execute(
                "ALTER TABLE patients ADD COLUMN date_of_birth TEXT NOT NULL DEFAULT ''"
            )
        if "profile_picture" not in columns:
            connection.execute("ALTER TABLE patients ADD COLUMN profile_picture BLOB")
        booking_columns = {row["name"] for row in connection.execute("PRAGMA table_info(bookings)")}
        doctor_columns = {row["name"] for row in connection.execute("PRAGMA table_info(doctors)")}
        for contact_table in ("admins", "doctors", "account_requests"):
            contact_columns = {row["name"] for row in connection.execute(f"PRAGMA table_info({contact_table})")}
            if "contact_email" not in contact_columns:
                connection.execute(f"ALTER TABLE {contact_table} ADD COLUMN contact_email TEXT NOT NULL DEFAULT ''")
        if "doctor_role" not in doctor_columns:
            connection.execute("ALTER TABLE doctors ADD COLUMN doctor_role TEXT NOT NULL DEFAULT 'Attending Physician'")
        if "registration_number" not in doctor_columns:
            connection.execute("ALTER TABLE doctors ADD COLUMN registration_number TEXT NOT NULL DEFAULT ''")
        if "residency_program" not in doctor_columns:
            connection.execute("ALTER TABLE doctors ADD COLUMN residency_program TEXT NOT NULL DEFAULT ''")
        if "residency_year" not in doctor_columns:
            connection.execute("ALTER TABLE doctors ADD COLUMN residency_year TEXT NOT NULL DEFAULT ''")
        if "doctor_id" not in booking_columns:
            connection.execute("ALTER TABLE bookings ADD COLUMN doctor_id TEXT REFERENCES doctors(doctor_id)")
        if "status" not in booking_columns:
            connection.execute("ALTER TABLE bookings ADD COLUMN status TEXT NOT NULL DEFAULT 'Scheduled'")
        if "appointment_time" not in booking_columns:
            connection.execute("ALTER TABLE bookings ADD COLUMN appointment_time TEXT NOT NULL DEFAULT 'Clinic hours: 8:00 AM - 3:00 PM'")
        if "confirmation_acknowledged_at" not in booking_columns:
            connection.execute("ALTER TABLE bookings ADD COLUMN confirmation_acknowledged_at TEXT")
        result_columns = {row["name"] for row in connection.execute("PRAGMA table_info(lab_results)")}
        if "pdf_filename" not in result_columns:
            connection.execute("ALTER TABLE lab_results ADD COLUMN pdf_filename TEXT")
        if "pdf_data" not in result_columns:
            connection.execute("ALTER TABLE lab_results ADD COLUMN pdf_data BLOB")
        if "file_mime" not in result_columns:
            connection.execute("ALTER TABLE lab_results ADD COLUMN file_mime TEXT")
        connection.execute(
            """CREATE TABLE IF NOT EXISTS result_access_log (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                subject TEXT NOT NULL,
                action TEXT NOT NULL,
                created_at TEXT NOT NULL
            )"""
        )
        connection.execute(
            "INSERT OR IGNORE INTO admins (admin_id, email, password_hash, display_name, created_at) VALUES (?, ?, ?, ?, ?)",
            ("ADMIN-001", "admin@gapcc.local", token_hash("admin123"), "GAPCC Administrator", utc_now()),
        )
        connection.execute(
            "INSERT OR IGNORE INTO doctors (doctor_id, email, password_hash, display_name, specialization, facility, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("DOC-001", "doctor@gapcc.local", token_hash("doctor123"), "Dr. April Michelle C.", "General Physician", FACILITIES[0], utc_now()),
        )
        doctor = connection.execute("SELECT doctor_id FROM doctors WHERE doctor_id = ?", ("DOC-001",)).fetchone()
        if doctor and connection.execute("SELECT 1 FROM doctor_schedules WHERE doctor_id = ?", (doctor["doctor_id"],)).fetchone() is None:
            connection.execute(
                "INSERT INTO doctor_schedules (doctor_id, day_of_week, start_time, end_time) VALUES (?, ?, ?, ?)",
                (doctor["doctor_id"], "Friday", "07:00", "16:00"),
            )
        default_packages = [("health", "Health Test Packages"), ("preemployment", "Pre-employment Packages"), ("specific", "Specific Health Packages"), ("ihc", "IHC Panel Packages"), ("autoimmune", "Autoimmune Packages")]
        for package_key, package_name in default_packages:
            connection.execute("INSERT OR IGNORE INTO clinic_packages (package_key, package_name, created_at, updated_at) VALUES (?, ?, ?, ?)", (package_key, package_name, utc_now(), utc_now()))
        package_columns = {row["name"] for row in connection.execute("PRAGMA table_info(clinic_packages)")}
        if "package_details" not in package_columns:
            connection.execute("ALTER TABLE clinic_packages ADD COLUMN package_details TEXT NOT NULL DEFAULT ''")
        connection.execute("UPDATE clinic_packages SET package_details = ? WHERE package_key = 'health' AND package_details = ''", ("Standard Chem Package | Laboratory: Fasting Blood Sugar; Blood Urea Nitrogen; Creatinine; Blood Uric Acid; Cholesterol; Triglycerides; Lipoprotein Profile (HDL/LDL/VLDL); SGPT; SGOT | Screening Test for Anemia | Laboratory: CBC with Platelet Count; Reticulocyte Count; TIBC + Fe; Ferritin; Peripheral Blood Smear | Optional: Vitamin B12; Hemoglobin Electrophoresis; Von Wilbrand Activity | Vitamin Panel | Laboratory: VITAMIN B12; VITAMIN A & 3; VITAMIN D2&D3",))
        connection.execute("UPDATE clinic_packages SET package_details = ? WHERE package_key = 'preemployment' AND package_details = ''", ("Standard Chem Package | Laboratory: CBC with Platelet Count; Complete Urinalysis; Fecalysis; Full Medical Exam | Imaging: Chest X-Ray PA | Optional: Drug Test (Meth & THC)",))
        connection.execute("UPDATE clinic_packages SET package_details = ? WHERE package_key = 'specific' AND package_details = ''", ("Prenatal Package | Laboratory: CBC with Platelet Count; Complete Urinalysis; HBsAg (RAPID) Screening; RPR; Anti-HBs (CMIA); HIV 1&2 (Quali); Blood & Rh Typing; Rubella IgG (CMIA); Fasting Blood Sugar | Dengue Screening Package | Laboratory: CBC with Platelet Count; Dengue NS1; Dengue IgG/IgM",))
        specific_package = connection.execute("SELECT package_details FROM clinic_packages WHERE package_key = 'specific'").fetchone()
        anemia_details = "Screening Test for Anemia | Laboratory: CBC with Platelet Count; Reticulocyte Count; TIBC + Fe; Ferritin; Peripheral Blood Smear | Optional: Vitamin B12; Hemoglobin Electrophoresis; Von Wilbrand Activity"
        if specific_package and "Screening Test for Anemia" not in specific_package["package_details"]:
            connection.execute("UPDATE clinic_packages SET package_details = package_details || ' | ' || ?, updated_at = ? WHERE package_key = 'specific'", (anemia_details, utc_now()))


initialize_database()


def option_markup(values):
    return "".join(
        f'<option value="{escape(value, quote=True)}">{escape(value)}</option>'
        for value in values
    )


def dashboard_count_link(href, icon, label, count, kind):
    badge_count = "99+" if count > 99 else str(count)
    return (
        f'<a class="dashboard-count-link {escape(kind, quote=True)}" href="{escape(href, quote=True)}" '
        f'title="{escape(label)}: {count}" aria-label="{escape(label)}: {count}">'
        f'<span class="dashboard-count-icon" aria-hidden="true">{escape(icon)}</span>'
        f'<span class="dashboard-count-badge" aria-hidden="true">{badge_count}</span>'
        f'<span class="sr-only">{escape(label)}: {count}</span></a>'
    )


def patient_name(patient):
    middle = f" {patient['middle_name']}" if patient["middle_name"] else ""
    return f"{patient['given_name']}{middle} {patient['last_name']}"


def patient_age(date_of_birth):
    if not date_of_birth:
        return "Not provided"
    birth_date = datetime.strptime(date_of_birth, "%Y-%m-%d").replace(tzinfo=timezone.utc).date()
    today = datetime.now(timezone.utc).date()
    return today.year - birth_date.year - (
        (today.month, today.day) < (birth_date.month, birth_date.day)
    )


ANALYTICS_BRACKETS = [("0 months - 21 yrs", 0, 21), ("23 - 59 yrs", 22, 59), ("60 - 85 yrs", 60, 200)]
ANALYTICS_GENDERS = ("Male", "Female")


def _analytics_age(date_of_birth, on_date):
    try:
        born = date.fromisoformat(date_of_birth)
    except (TypeError, ValueError):
        return None
    return on_date.year - born.year - ((on_date.month, on_date.day) < (born.month, born.day))


def _analytics_period(rows, start, end, buckets, bucket_of):
    series = [{"label": label, "counts": [[0, 0] for _ in ANALYTICS_BRACKETS]} for label in buckets]
    skipped = 0
    for row in rows:
        try:
            when = date.fromisoformat(row["appointment_date"])
        except ValueError:
            continue
        if not start <= when <= end:
            continue
        gender = (row["gender"] or "").strip().title()
        age = _analytics_age(row["date_of_birth"], when)
        if gender not in ANALYTICS_GENDERS or age is None:
            skipped += 1
            continue
        index = next(i for i, (_, low, high) in enumerate(ANALYTICS_BRACKETS) if low <= age <= high or (i == len(ANALYTICS_BRACKETS) - 1 and age > high))
        series[bucket_of(when)]["counts"][index][ANALYTICS_GENDERS.index(gender)] += 1
    return series, skipped


def _analytics_totals(series):
    totals = [[0, 0] for _ in ANALYTICS_BRACKETS]
    for bucket in series:
        for i, pair in enumerate(bucket["counts"]):
            totals[i][0] += pair[0]
            totals[i][1] += pair[1]
    return totals


def _analytics_insights(name, label, series, totals, previous_total, skipped):
    male = sum(t[0] for t in totals)
    female = sum(t[1] for t in totals)
    total = male + female
    if not total:
        return [f"No bookings with complete gender and age details were recorded for {label} ({name})."]
    notes = [f"{total} booking{'s' if total != 1 else ''} were recorded {label}: {male} Male ({round(male * 100 / total)}%) and {female} Female ({round(female * 100 / total)}%)."]
    group_totals = [t[0] + t[1] for t in totals]
    top = max(range(len(group_totals)), key=lambda i: group_totals[i])
    notes.append(f"The largest age group is {ANALYTICS_BRACKETS[top][0]} with {group_totals[top]} booking{'s' if group_totals[top] != 1 else ''} ({round(group_totals[top] * 100 / total)}%).")
    if male != female:
        lead = "Male" if male > female else "Female"
        notes.append(f"{lead} patients lead overall by {abs(male - female)} booking{'s' if abs(male - female) != 1 else ''}.")
    else:
        notes.append("Male and Female bookings are evenly split.")
    busy = max(series, key=lambda b: sum(sum(pair) for pair in b["counts"]))
    busy_total = sum(sum(pair) for pair in busy["counts"])
    if busy_total:
        notes.append(f"The busiest period is {busy['label']} with {busy_total} booking{'s' if busy_total != 1 else ''}.")
    empty = [ANALYTICS_BRACKETS[i][0] for i, v in enumerate(group_totals) if v == 0]
    if empty:
        notes.append("No bookings from: " + ", ".join(empty) + ".")
    if previous_total:
        change = round((total - previous_total) * 100 / previous_total)
        direction = "up" if change > 0 else "down" if change < 0 else "unchanged"
        notes.append(f"Compared with the previous {name} ({previous_total} bookings), volume is {direction}" + (f" {abs(change)}%." if change else "."))
    else:
        notes.append(f"There is no previous {name} data to compare against.")
    if skipped:
        notes.append(f"{skipped} booking(s) were left out because the patient's gender or birthday is missing or not Male/Female.")
    return notes


def build_booking_analytics(connection):
    import json as _json

    rows = connection.execute(
        "SELECT b.appointment_date, p.gender, p.date_of_birth FROM bookings b JOIN patients p ON p.patient_id = b.patient_id WHERE b.status != 'Cancelled'"
    ).fetchall()
    today = datetime.now(timezone.utc).date()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    month_start = today.replace(day=1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    month_end = next_month - timedelta(days=1)
    prev_month_end = month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    week_labels = [f"{days[i]} {(week_start + timedelta(days=i)).strftime('%b %d')}" for i in range(7)]
    week, week_skipped = _analytics_period(rows, week_start, week_end, week_labels, lambda d: d.weekday())
    prev_week, _ = _analytics_period(rows, week_start - timedelta(days=7), week_start - timedelta(days=1), week_labels, lambda d: d.weekday())
    count = (month_end.day + 6) // 7
    month_labels = [f"Week {i + 1} ({i * 7 + 1}-{min(i * 7 + 7, month_end.day)})" for i in range(count)]
    month, month_skipped = _analytics_period(rows, month_start, month_end, month_labels, lambda d: (d.day - 1) // 7)
    prev_month, _ = _analytics_period(rows, prev_month_start, prev_month_end, ["x"] * 6, lambda d: (d.day - 1) // 7)

    def total_of(series):
        return sum(sum(sum(pair) for pair in bucket["counts"]) for bucket in series)

    periods = {}
    for key, name, label, series, prev, skipped, rng in [
        ("week", "week", "this week", week, prev_week, week_skipped, f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}"),
        ("month", "month", "this month", month, prev_month, month_skipped, today.strftime("%B %Y")),
    ]:
        totals = _analytics_totals(series)
        periods[key] = {
            "range": rng,
            "series": series,
            "totals": totals,
            "insights": _analytics_insights(name, label, series, totals, total_of(prev), skipped),
        }
    data = _json.dumps({"brackets": [b[0] for b in ANALYTICS_BRACKETS], "genders": list(ANALYTICS_GENDERS), "periods": periods}).replace("</", "<\\/")
    return ANALYTICS_HTML + "<script>window.GAPCC_ANALYTICS=" + data + ";" + ANALYTICS_SCRIPT + "</script>"


ANALYTICS_HTML = """<style>
.bk-analytics .bk-controls{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin:10px 0 14px}
.bk-analytics .bk-seg{display:inline-flex;border:1px solid #cfe0dd;border-radius:8px;overflow:hidden}
.bk-analytics .bk-seg button{border:0;background:#fff;padding:8px 14px;font-weight:700;cursor:pointer;color:#26504d;margin:0;border-radius:0;width:auto}
.bk-analytics .bk-seg button.on{background:#087e7a;color:#fff}
.bk-analytics select{padding:8px;border:1px solid #cfe0dd;border-radius:8px;width:auto}
.bk-analytics .bk-legend{display:flex;gap:14px;font-weight:700;font-size:13px}
.bk-analytics .bk-legend label{display:flex;align-items:center;gap:6px;cursor:pointer}
.bk-analytics .bk-legend i{width:12px;height:12px;border-radius:3px;display:inline-block}
.bk-analytics .bk-grid{display:grid;grid-template-columns:minmax(0,3fr) minmax(0,2fr);gap:18px}
@media(max-width:900px){.bk-analytics .bk-grid{grid-template-columns:1fr}}
.bk-analytics .bk-chart{position:relative;background:#f7fbfa;border:1px solid #dcebe8;border-radius:10px;padding:10px;min-height:300px}
.bk-analytics svg{width:100%;height:auto;display:block}
.bk-analytics .bk-bar{cursor:pointer;transition:opacity .15s}.bk-analytics .bk-bar:hover{opacity:.75}
.bk-analytics .bk-tip{position:absolute;pointer-events:none;background:#0b2b3a;color:#fff;font-size:12px;padding:6px 9px;border-radius:6px;display:none;white-space:nowrap;z-index:5}
.bk-analytics .bk-summary{background:#f0f8f7;border:1px solid #dcebe8;border-radius:10px;padding:14px 16px}
.bk-analytics .bk-summary h3{margin:0 0 8px;font-size:15px}.bk-analytics .bk-summary ul{margin:0;padding-left:18px;line-height:1.55;font-size:14px}
.bk-analytics .bk-stats{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}
.bk-analytics .bk-stats span{background:#fff;border:1px solid #dcebe8;border-radius:8px;padding:6px 10px;font-size:13px}
</style>
<section class="table-card bk-analytics" id="admin-analytics" data-admin-section><div class="table-heading"><h2>Booking Analytics</h2><span id="bk-range"></span></div>
<div class="bk-controls"><div class="bk-seg" role="group" aria-label="Report period"><button type="button" data-bk-period="week" class="on">Weekly</button><button type="button" data-bk-period="month">Monthly</button></div>
<div class="bk-seg" role="group" aria-label="Chart view"><button type="button" data-bk-view="age" class="on">By age bracket</button><button type="button" data-bk-view="time">Over time</button></div>
<select id="bk-bracket" aria-label="Age bracket filter" hidden></select>
<div class="bk-legend"><label><input type="checkbox" data-bk-gender="0" checked><i style="background:#26658C"></i>Male</label><label><input type="checkbox" data-bk-gender="1" checked><i style="background:#e0678f"></i>Female</label></div></div>
<div class="bk-grid"><div class="bk-chart"><div id="bk-svg"></div><div class="bk-tip" id="bk-tip"></div></div>
<div class="bk-summary"><h3 id="bk-title">Automated summary</h3><ul id="bk-insights"></ul><div class="bk-stats" id="bk-stats"></div></div></div></section>"""

ANALYTICS_SCRIPT = """(function(){
var D=window.GAPCC_ANALYTICS,st={p:'week',v:'age',g:[true,true],b:-1},C=['#26658C','#e0678f'],NS='http://www.w3.org/2000/svg';
var sel=document.getElementById('bk-bracket');
sel.innerHTML='<option value="-1">All age brackets</option>'+D.brackets.map(function(b,i){return '<option value="'+i+'">'+b+'</option>';}).join('');
function el(n,a,t){var e=document.createElementNS(NS,n);for(var k in a)e.setAttribute(k,a[k]);if(t!=null)e.textContent=t;return e;}
function data(){var P=D.periods[st.p],cats=[],vals=[];
if(st.v==='age'){D.brackets.forEach(function(b,i){cats.push(b);vals.push(P.totals[i].slice());});}
else{P.series.forEach(function(s){var m=0,f=0;s.counts.forEach(function(c,i){if(st.b<0||st.b===i){m+=c[0];f+=c[1];}});cats.push(s.label);vals.push([m,f]);});}
return {cats:cats,vals:vals};}
function draw(){var d=data(),W=640,H=320,L=40,B=64,T=16,R=10,pw=W-L-R,ph=H-T-B,max=1;
d.vals.forEach(function(v){v.forEach(function(x,g){if(st.g[g]&&x>max)max=x;});});
max=Math.max(4,Math.ceil(max/4)*4);
var s=el('svg',{viewBox:'0 0 '+W+' '+H,role:'img','aria-label':'Booking analytics chart'});
for(var i=0;i<=4;i++){var y=T+ph-ph*i/4;s.appendChild(el('line',{x1:L,x2:W-R,y1:y,y2:y,stroke:'#dbe7e5'}));s.appendChild(el('text',{x:L-6,y:y+4,'text-anchor':'end','font-size':11,fill:'#5b7270'},Math.round(max*i/4)));}
var n=d.cats.length,gw=pw/n,active=[0,1].filter(function(g){return st.g[g];}),bw=Math.min(34,(gw*0.7)/Math.max(1,active.length)),tip=document.getElementById('bk-tip'),box=document.getElementById('bk-svg').parentNode;
d.cats.forEach(function(c,ci){var x0=L+gw*ci+(gw-bw*active.length)/2;
active.forEach(function(g,k){var v=d.vals[ci][g],h=ph*v/max,x=x0+bw*k,y=T+ph-h;
var r=el('rect',{x:x,y:y,width:bw-3,height:Math.max(h,0),rx:3,fill:C[g],'class':'bk-bar'});
r.addEventListener('mousemove',function(e){var b=box.getBoundingClientRect();tip.style.display='block';tip.textContent=c+' - '+D.genders[g]+': '+v;tip.style.left=Math.min(e.clientX-b.left+12,b.width-170)+'px';tip.style.top=(e.clientY-b.top-30)+'px';});
r.addEventListener('mouseleave',function(){tip.style.display='none';});
s.appendChild(r);if(v)s.appendChild(el('text',{x:x+(bw-3)/2,y:y-4,'text-anchor':'middle','font-size':11,'font-weight':700,fill:'#26504d'},v));});
var lbl=el('text',{x:L+gw*ci+gw/2,y:H-B+16,'text-anchor':'middle','font-size':n>6?10:12,fill:'#26504d'});
var parts=c.split(' (');lbl.appendChild(el('tspan',{x:L+gw*ci+gw/2,dy:0},parts[0]));if(parts[1])lbl.appendChild(el('tspan',{x:L+gw*ci+gw/2,dy:13},'('+parts[1]));s.appendChild(lbl);});
var holder=document.getElementById('bk-svg');holder.innerHTML='';holder.appendChild(s);}
function summary(){var P=D.periods[st.p],tm=0,tf=0;P.totals.forEach(function(t){tm+=t[0];tf+=t[1];});
document.getElementById('bk-range').textContent=(st.p==='week'?'Weekly report: ':'Monthly report: ')+P.range;
document.getElementById('bk-title').textContent=(st.p==='week'?'Weekly':'Monthly')+' summary & key insights';
document.getElementById('bk-insights').innerHTML=P.insights.map(function(t){return '<li>'+t.replace(/&/g,'&amp;').replace(/</g,'&lt;')+'</li>';}).join('');
document.getElementById('bk-stats').innerHTML='<span>Total: <b>'+(tm+tf)+'</b></span><span>Male: <b>'+tm+'</b></span><span>Female: <b>'+tf+'</b></span>';}
function render(){draw();summary();}
document.addEventListener('click',function(e){var b=e.target.closest('[data-bk-period],[data-bk-view]');if(!b)return;
if(b.dataset.bkPeriod)st.p=b.dataset.bkPeriod;if(b.dataset.bkView){st.v=b.dataset.bkView;sel.hidden=st.v!=='time';}
b.parentNode.querySelectorAll('button').forEach(function(x){x.classList.toggle('on',x===b);});render();});
document.addEventListener('change',function(e){if(e.target.dataset&&e.target.dataset.bkGender!=null){st.g[+e.target.dataset.bkGender]=e.target.checked;render();}
if(e.target===sel){st.b=+sel.value;render();}});
render();})();"""


RESULT_SECRET = (os.environ.get("GAPCC_RESULT_SECRET") or secrets.token_hex(32)).encode("utf-8")
RESULT_TOKEN_TTL = 3600
PORTAL_SESSION_TTL = 30 * 24 * 60 * 60
PORTAL_SESSION_COOKIE = "gapcc_portal_session"
MAX_RESULT_BYTES = 10 * 1024 * 1024
RESULT_MIME = {"pdf": "application/pdf", "png": "image/png", "jpeg": "image/jpeg"}
SECURITY_HEADERS = {
    "Cache-Control": "no-store, private",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "X-Frame-Options": "SAMEORIGIN",
}


def sign_access_token(role, subject, ttl=RESULT_TOKEN_TTL):
    expires = int(datetime.now(timezone.utc).timestamp()) + ttl
    payload = base64.urlsafe_b64encode(f"{role}|{subject}|{expires}".encode("utf-8")).decode("ascii").rstrip("=")
    signature = hmac.new(RESULT_SECRET, payload.encode("ascii"), "sha256").hexdigest()
    return f"{payload}.{signature}"


def verify_access_token(token):
    try:
        payload, signature = (token or "").split(".", 1)
        expected = hmac.new(RESULT_SECRET, payload.encode("ascii"), "sha256").hexdigest()
        if not hmac.compare_digest(signature, expected):
            return None
        role, subject, expires = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)).decode("utf-8").split("|")
        if int(expires) < datetime.now(timezone.utc).timestamp():
            return None
        return role, subject
    except (ValueError, UnicodeError):
        return None


def set_portal_session(response, role, subject):
    response.set_cookie(
        PORTAL_SESSION_COOKIE,
        sign_access_token(role, subject, PORTAL_SESSION_TTL),
        max_age=PORTAL_SESSION_TTL,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return response


def clear_portal_session(response):
    response.delete_cookie(PORTAL_SESSION_COOKIE, path="/", httponly=True, samesite="lax")
    return response


def sniff_result_type(data):
    if data.startswith(b"%PDF-"):
        return "pdf"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if data.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    return None


def validate_result_upload(data):
    kind = sniff_result_type(data)
    if kind is None:
        return None, "Upload a PDF, PNG, or JPEG file only."
    if len(data) > MAX_RESULT_BYTES:
        return None, "The file is larger than 10 MB."
    try:
        if kind == "pdf":
            from pypdf import PdfReader

            reader = PdfReader(io.BytesIO(data))
            if reader.is_encrypted and not reader.decrypt(""):
                return None, "Password-protected PDFs cannot be watermarked. Remove the password and upload again."
            if not 0 < len(reader.pages) <= 100:
                return None, "The PDF must have between 1 and 100 pages."
        else:
            from PIL import Image

            image = Image.open(io.BytesIO(data))
            image.verify()
    except Exception:
        return None, "The file is damaged or could not be read."
    return kind, ""


def doctor_watermark_text(name, license_no):
    name = (name or "").strip()
    if name and not re.match(r"^dr\.?\s", name, re.IGNORECASE):
        name = f"Dr. {name}"
    return f"Handled by: {name or 'Unassigned'} | License No: {(license_no or '').strip() or 'Not on file'}"


def _pdf_safe(text):
    return str(text).encode("latin-1", "replace").decode("latin-1")


def _result_overlay(width, height, header, footer, watermark, info, page_no, page_count):
    from math import hypot
    from reportlab.pdfbase.pdfmetrics import stringWidth
    from reportlab.pdfgen import canvas

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=(width, height))
    scale = max(0.6, min(1.5, width / 595))
    pdf.setFillColorRGB(0.0, 0.22, 0.35)
    pdf.rect(0, height - header, width, header, stroke=0, fill=1)
    pdf.setFillColorRGB(0.33, 0.67, 0.75)
    pdf.rect(0, height - header - 3 * scale, width, 3 * scale, stroke=0, fill=1)
    pdf.setFillColorRGB(1, 1, 1)
    pdf.setFont("Helvetica-Bold", 20 * scale)
    pdf.drawString(24 * scale, height - 30 * scale, "GAPCC")
    pdf.setFont("Helvetica", 9.5 * scale)
    pdf.drawString(24 * scale, height - 46 * scale, "Gavino Y. Alvarez Primary Care Center")
    pdf.setFont("Helvetica", 8 * scale)
    pdf.drawString(24 * scale, height - 60 * scale, "Diagnostic & Urgent Care Center")
    pdf.setFont("Helvetica-Bold", 8.5 * scale)
    pdf.drawRightString(width - 24 * scale, height - 24 * scale, "OFFICIAL LABORATORY / DIAGNOSTIC RESULT")
    pdf.setFont("Helvetica", 8 * scale)
    for index, line in enumerate(info):
        pdf.drawRightString(width - 24 * scale, height - (38 + index * 11) * scale, _pdf_safe(line))
    pdf.setFillColorRGB(0.35, 0.4, 0.42)
    pdf.setFont("Helvetica", 7 * scale)
    pdf.drawString(24 * scale, 7 * scale, _pdf_safe(f"{watermark}  -  System-generated official GAPCC document"))
    pdf.drawRightString(width - 24 * scale, 7 * scale, f"Page {page_no} of {page_count}")
    text = _pdf_safe(watermark)
    diagonal = hypot(width, height)
    size = 30.0
    while size > 8 and stringWidth(text, "Helvetica-Bold", size) > diagonal * 0.46:
        size -= 1
    pdf.saveState()
    pdf.setFillColorRGB(0.02, 0.30, 0.42)
    pdf.setFillAlpha(0.2)
    pdf.setFont("Helvetica-Bold", size)
    pdf.translate(width / 2, (height - header + footer) / 2)
    pdf.rotate(35)
    step = max(110.0, height / 4)
    offset = -step * 2
    while offset <= step * 2:
        pdf.drawCentredString(0, offset, text)
        offset += step
    pdf.restoreState()
    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return buffer


def build_watermarked_result(row):
    from pypdf import PageObject, PdfReader, PdfWriter

    data = bytes(row["pdf_data"])
    kind = sniff_result_type(data)
    watermark = doctor_watermark_text(row["doctor_name"], row["license_no"])
    patient = patient_name(row)
    info = [f"Patient: {patient}", f"Patient ID: {row['patient_id']}  |  Result No. {row['result_id']}", f"Test: {row['test_name']}  |  Date: {row['collected_at']}"]
    writer = PdfWriter()
    if kind == "pdf":
        reader = PdfReader(io.BytesIO(data))
        if reader.is_encrypted:
            reader.decrypt("")
        count = len(reader.pages)
        for number, source in enumerate(reader.pages, start=1):
            source.transfer_rotation_to_content()
            box = source.mediabox
            width, height = float(box.width), float(box.height)
            scale = max(0.6, min(1.5, width / 595))
            header, footer = round(76 * scale), round(18 * scale)
            canvas_page = PageObject.create_blank_page(None, width, height + header + footer)
            canvas_page.merge_translated_page(source, -float(box.left), -float(box.bottom) + footer)
            overlay = PdfReader(_result_overlay(width, height + header + footer, header, footer, watermark, info, number, count)).pages[0]
            canvas_page.merge_page(overlay)
            writer.add_page(canvas_page)
    else:
        from PIL import Image, ImageOps
        from reportlab.lib.utils import ImageReader
        from reportlab.pdfgen import canvas

        image = ImageOps.exif_transpose(Image.open(io.BytesIO(data))).convert("RGB")
        image.thumbnail((2600, 2600))
        width, height, header, footer = 595.0, 842.0, 76, 18
        pdf_buffer = io.BytesIO()
        base = canvas.Canvas(pdf_buffer, pagesize=(width, height))
        box_w, box_h = width - 56, height - header - footer - 40
        ratio = min(box_w / image.width, box_h / image.height)
        draw_w, draw_h = image.width * ratio, image.height * ratio
        base.drawImage(ImageReader(image), (width - draw_w) / 2, footer + 20 + (box_h - draw_h) / 2, draw_w, draw_h)
        base.showPage()
        base.save()
        pdf_buffer.seek(0)
        page_one = PdfReader(pdf_buffer).pages[0]
        page_one.merge_page(PdfReader(_result_overlay(width, height, header, footer, watermark, info, 1, 1)).pages[0])
        writer.add_page(page_one)
    writer.add_metadata({"/Title": f"GAPCC result {row['result_id']}", "/Author": "GAPCC", "/Subject": watermark})
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def load_result_row(connection, result_id):
    return connection.execute(
        """SELECT l.*, p.given_name, p.middle_name, p.last_name, d.display_name AS doctor_name, d.registration_number AS license_no
        FROM lab_results l JOIN patients p ON p.patient_id = l.patient_id
        LEFT JOIN doctors d ON d.doctor_id = l.doctor_id WHERE l.result_id = ?""",
        (result_id,),
    ).fetchone()


def authorize_result_access(connection, token, result):
    claim = verify_access_token(token)
    if claim is None or result is None or not result["pdf_data"]:
        return None
    role, subject = claim
    if role == "patient":
        allowed = subject == result["patient_id"] and result["result_status"] == "Released" and connection.execute(
            "SELECT 1 FROM patients WHERE patient_id = ? AND portal_activated = 1", (subject,)
        ).fetchone()
    elif role == "doctor":
        allowed = subject == result["doctor_id"] and connection.execute("SELECT 1 FROM doctors WHERE doctor_id = ? AND active = 1", (subject,)).fetchone()
    elif role == "admin":
        allowed = connection.execute("SELECT 1 FROM admins WHERE admin_id = ?", (subject,)).fetchone()
    else:
        allowed = False
    return (role, subject) if allowed else None


def log_result_access(connection, result_id, role, subject, action):
    connection.execute(
        "INSERT INTO result_access_log (result_id, role, subject, action, created_at) VALUES (?, ?, ?, ?, ?)",
        (result_id, role, subject, action, utc_now()),
    )


def result_action_links(result_id, token):
    base = f"/results/{result_id}"
    safe = escape(token, quote=True)
    return (
        f'<a class="rx-act" href="{base}/view?t={safe}" target="_blank" rel="noopener">View</a>'
        f'<a class="rx-act" href="{base}/file?t={safe}&amp;dl=1">Download</a>'
        f'<a class="rx-act" href="{base}/view?t={safe}&amp;print=1" target="_blank" rel="noopener">Print</a>'
    )


def result_document_preview(result_id, token, test_name):
    safe_token = escape(token, quote=True)
    file_url = f"/results/{result_id}/file?t={safe_token}"
    return (
        f'<div class="patient-result-actions">{result_action_links(result_id, token)}</div>'
        f'<iframe class="patient-result-preview" src="{file_url}" '
        f'title="Uploaded laboratory result: {escape(test_name, quote=True)}" loading="lazy"></iframe>'
    )


RESULT_ACTION_CSS = """.rx-act{display:inline-block;margin:0 4px 4px 0;padding:5px 10px;border:1px solid #087e7a;border-radius:6px;color:#087e7a;font-weight:700;font-size:12px;text-decoration:none;background:#fff}.rx-act:hover{background:#087e7a;color:#fff}
@media print{.doctor-sidebar,.doctor-topbar,.admin-sidebar,nav,button,.button,.rx-act,form,.rx-drop{display:none!important}}"""

RESULT_UPLOAD_CSS = """
.rx-drop{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;margin:8px 0 14px;padding:26px 16px;border:2px dashed #54ACBF;border-radius:12px;background:#f2fafc;color:#26658C;text-align:center;cursor:pointer;transition:background .15s,border-color .15s}
.rx-drop.is-over{background:#dff3f8;border-color:#087e7a}.rx-drop strong{font-size:15px}.rx-drop small{color:#5b7270}
.rx-drop input[type=file]{position:absolute;width:1px;height:1px;opacity:0}
.rx-preview{display:flex;align-items:center;gap:10px;margin-top:6px;font-weight:700;color:#023859}.rx-preview img{max-height:70px;border-radius:6px;border:1px solid #cfe0dd}
.rx-error{color:#b42318;font-weight:700;margin:4px 0}
.rx-attending{display:flex;flex-wrap:wrap;gap:8px 22px;padding:10px 14px;margin:8px 0 12px;border-radius:10px;background:#eef7f6;border:1px solid #cfe0dd;font-size:13px}
.rx-attending span{color:#5b7270}.rx-attending strong{color:#023859}
.rx-warn{padding:10px 14px;border-radius:10px;background:#fff7e6;border:1px solid #f1d28a;margin:8px 0 12px}
"""

RESULT_UPLOAD_SCRIPT = """<script>(function(){if(window.__rxUpload)return;window.__rxUpload=true;
var MAX=10*1024*1024,OK=['application/pdf','image/png','image/jpeg'];
function zone(t){return t.closest&&t.closest('.rx-drop');}
function show(z,file){var box=z.parentNode.querySelector('.rx-preview'),err=z.parentNode.querySelector('.rx-error');box.innerHTML='';err.textContent='';
if(!file)return;var input=z.querySelector('input');
if(OK.indexOf(file.type)<0||file.size>MAX){input.value='';err.textContent=OK.indexOf(file.type)<0?'Only PDF, PNG, or JPEG files are accepted.':'The file must be smaller than 10 MB.';return;}
var label=document.createElement('span');label.textContent=file.name+' ('+Math.max(1,Math.round(file.size/1024))+' KB)';
if(file.type.indexOf('image/')===0){var img=document.createElement('img');img.src=URL.createObjectURL(file);box.appendChild(img);}box.appendChild(label);}
['dragenter','dragover'].forEach(function(n){document.addEventListener(n,function(e){var z=zone(e.target);if(z){e.preventDefault();z.classList.add('is-over');}});});
['dragleave','drop'].forEach(function(n){document.addEventListener(n,function(e){var z=zone(e.target);if(z){z.classList.remove('is-over');}});});
document.addEventListener('drop',function(e){var z=zone(e.target);if(!z)return;e.preventDefault();var f=e.dataTransfer.files;if(f&&f.length){var input=z.querySelector('input');var dt=new DataTransfer();dt.items.add(f[0]);input.files=dt.files;show(z,f[0]);}});
document.addEventListener('change',function(e){var z=zone(e.target);if(z)show(z,e.target.files[0]);});
})();</script>"""


def doctor_results_panel(doctor, patient_options, own_results, token):
    license_no = (doctor["registration_number"] or "").strip()
    safe_token = escape(token, quote=True)
    if license_no:
        license_block = ""
    else:
        license_block = (
            '<div class="rx-warn"><strong>License number required.</strong> Results are watermarked with your PRC / medical license number. '
            f'<form action="/doctor/license" method="post"><input type="hidden" name="doctor_token" value="{safe_token}">'
            '<label for="rx_license">PRC / License number</label><input id="rx_license" name="registration_number" required maxlength="40">'
            '<button class="button" type="submit">Save license number</button></form></div>'
        )
    rows = "".join(
        f"<tr><td>{escape(row['collected_at'])}</td><td>{escape(patient_name(row))}<span class=\"secondary-cell\"> {escape(row['patient_id'])}</span></td><td>{escape(row['test_name'])}</td><td>{result_action_links(row['result_id'], token)}</td></tr>"
        for row in own_results
    ) or '<tr><td colspan="4">No documents uploaded yet.</td></tr>'
    disabled = "" if license_no else " disabled"
    return (
        f'<section class="doctor-panel" id="doctor-results"><style>{RESULT_UPLOAD_CSS}{RESULT_ACTION_CSS}</style>'
        '<div class="panel-heading"><h2>Upload Laboratory / Diagnostic Result</h2><small>Watermarked automatically</small></div>'
        f'<div class="rx-attending"><div><span>Attending doctor</span><br><strong>{escape(doctor["display_name"])}</strong></div>'
        f'<div><span>PRC / License No.</span><br><strong>{escape(license_no or "Not on file")}</strong></div></div>{license_block}'
        f'<form action="/doctor/results" method="post" enctype="multipart/form-data"><input type="hidden" name="doctor_token" value="{safe_token}">'
        '<div class="grid"><div><label for="result_patient">Patient ID</label>'
        f'<select id="result_patient" name="patient_id" required>{patient_options}</select></div>'
        '<div><label for="test_name">Test / Diagnostic Name</label><input id="test_name" name="test_name" placeholder="Complete Blood Count" required></div></div>'
        '<label for="result_summary">Result Summary</label><textarea id="result_summary" name="result_summary" rows="3" required></textarea>'
        '<div><label class="rx-drop" for="result_file"><strong>Drag and drop the result here, or click to browse</strong>'
        '<small>PDF, PNG or JPEG - max 10 MB - a diagonal doctor watermark is added automatically</small>'
        '<input id="result_file" type="file" name="result_file" accept="application/pdf,image/png,image/jpeg,.pdf,.png,.jpg,.jpeg" required></label>'
        '<div class="rx-preview"></div><div class="rx-error" role="alert"></div></div>'
        f'<button class="button green" type="submit"{disabled}>Upload and Release to Patient</button></form>'
        '<div class="panel-heading" style="margin-top:22px"><h2>My Uploaded Results</h2><small>View, download or print</small></div>'
        f'<div class="table-scroll"><table><thead><tr><th>Date</th><th>Patient</th><th>Test</th><th>Document</th></tr></thead><tbody>{rows}</tbody></table></div>'
        f'{RESULT_UPLOAD_SCRIPT}</section>'
    )


RESULT_VIEWER_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="referrer" content="no-referrer"><title>GAPCC Result __ID__</title><style>
html,body{margin:0;height:100%;background:#dfe8ea;font-family:Segoe UI,Arial,sans-serif}
.bar{display:flex;flex-wrap:wrap;gap:10px;align-items:center;padding:10px 16px;background:#023859;color:#fff}
.bar strong{margin-right:auto}.bar a,.bar button{border:0;border-radius:6px;padding:8px 14px;font-weight:700;font-size:14px;cursor:pointer;text-decoration:none;background:#54ACBF;color:#011C40}
.bar button.print{background:#fff}
iframe{display:block;width:100%;height:calc(100vh - 54px);border:0;background:#fff}
@media print{.bar{display:none!important}html,body{background:#fff;height:auto}iframe{height:100vh}@page{margin:0}}
</style></head><body><div class="bar"><strong>Official result - __WATERMARK__</strong>
<button class="print" type="button" id="p">Print</button><a href="__DOWNLOAD__">Download PDF</a><a href="javascript:window.close()">Close</a></div>
<iframe id="doc" title="Official result document" src="__FILE__"></iframe>
<script>(function(){var f=document.getElementById('doc');function go(){try{f.contentWindow.focus();f.contentWindow.print();}catch(e){window.open(f.src,'_blank');}}
document.getElementById('p').addEventListener('click',go);
if(__AUTOPRINT__){f.addEventListener('load',function(){setTimeout(go,600);});}})();</script></body></html>"""


def date_string_to_date(value):
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def appointment_slot_count(connection, doctor_id, appointment_date, appointment_time, exclude_booking_id=None):
    query = """SELECT COUNT(*) AS total FROM bookings
        WHERE doctor_id = ? AND appointment_date = ? AND appointment_time = ?
        AND status != 'Cancelled'"""
    parameters = [doctor_id, appointment_date, appointment_time]
    if exclude_booking_id is not None:
        query += " AND booking_id != ?"
        parameters.append(exclude_booking_id)
    return connection.execute(query, parameters).fetchone()["total"]


def patient_for_portal_credentials(connection, patient_id, email, contact_number):
    normalized_contact = normalize_philippine_number(contact_number)
    if normalized_contact is None:
        return None
    legacy_local = f"0{normalized_contact[3:]}"
    legacy_international = normalized_contact[1:]
    return connection.execute(
        """SELECT patient_id, email, contact_number FROM patients
        WHERE patient_id = ? AND email = ? AND contact_number IN (?, ?, ?) AND portal_activated = 1""",
        (patient_id.strip(), email.strip().lower(), normalized_contact, legacy_local, legacy_international),
    ).fetchone()


def notify_patient_booking_change(connection, booking, patient, action, doctor_name):
    now = datetime.now(timezone.utc)
    connection.execute(
        """UPDATE appointment_messages SET status = 'Cancelled'
        WHERE booking_id = ? AND message_type = 'Reminder' AND status IN ('Scheduled', 'Queued', 'Pending')""",
        (booking["booking_id"],),
    )
    if action != "cancelled":
        reminder = (
            f"GAPCC reminder: your {booking['service']} appointment is tomorrow at {booking['facility']}. "
            f"Confirmation number: {booking['token_id']}. {booking['appointment_time']}."
        )
        reminder_time = datetime.combine(date.fromisoformat(booking["appointment_date"]), datetime.min.time(), timezone.utc) - timedelta(days=1)
        connection.execute(
            """INSERT INTO appointment_messages
            (booking_id, patient_id, message_type, channel, destination, message_body, scheduled_for, sent_at, status, created_at)
            VALUES (?, ?, 'Reminder', 'Email', ?, ?, ?, NULL, 'Scheduled', ?)""",
            (booking["booking_id"], patient["patient_id"], patient["email"], reminder, reminder_time.isoformat(), now.isoformat()),
        )
    appointment_label = date.fromisoformat(booking["appointment_date"]).strftime("%B %d, %Y").replace(" 0", " ")
    body = (
        f"Your GAPCC appointment {booking['token_id']} was {action}. "
        f"Service: {booking['service']}. Facility: {booking['facility']}. "
        f"Date: {appointment_label}. {booking['appointment_time']}. "
        f"Assigned doctor: {doctor_name}."
    )
    message_cursor = connection.execute(
        """INSERT INTO appointment_messages
        (booking_id, patient_id, message_type, channel, destination, message_body, scheduled_for, sent_at, status, created_at)
        VALUES (?, ?, 'Update', 'Email', ?, ?, ?, NULL, 'Pending', ?)""",
        (booking["booking_id"], patient["patient_id"], patient["email"], body, now.isoformat(), now.isoformat()),
    )
    delivery_status = send_email_message(patient["email"], "GAPCC appointment update", body)
    connection.execute(
        "UPDATE appointment_messages SET status = ?, sent_at = ? WHERE message_id = ?",
        (delivery_status, now.isoformat() if delivery_status == "Sent" else None, message_cursor.lastrowid),
    )


BOOKING_AVAILABILITY_SCRIPT = """
document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('[data-slot-availability-form]').forEach(function (form) {
        const doctor = form.querySelector('[data-slot-doctor]');
        const appointmentDate = form.querySelector('[data-slot-date]');
        const appointmentTime = form.querySelector('[data-slot-time]');
        const availability = form.querySelector('[data-slot-availability]');
        const submit = form.querySelector('[data-slot-submit]');
        const exclude = form.querySelector('[data-slot-exclude]');
        let requestNumber = 0;
        async function refreshAvailability() {
            if (!doctor.value || !appointmentDate.value || !appointmentTime.value) {
                availability.textContent = 'Choose a doctor and date to check live availability.';
                if (submit) submit.disabled = true;
                return;
            }
            const currentRequest = ++requestNumber;
            availability.textContent = 'Checking availability...';
            const params = new URLSearchParams({
                doctor_id: doctor.value,
                appointment_date: appointmentDate.value,
                appointment_time: appointmentTime.value
            });
            if (exclude && exclude.value) params.set('exclude_booking_id', exclude.value);
            try {
                const response = await fetch('/availability?' + params.toString(), {headers: {'Accept': 'application/json'}});
                const data = await response.json();
                if (!response.ok) throw new Error(data.detail || 'Availability could not be checked.');
                if (currentRequest !== requestNumber) return;
                availability.textContent = data.available
                    ? data.remaining + ' of ' + data.capacity + ' appointments available.'
                    : 'This appointment slot is full. Choose another slot.';
                if (submit) submit.disabled = !data.available;
            } catch (error) {
                if (currentRequest !== requestNumber) return;
                availability.textContent = 'Live availability could not be checked. The server will verify the slot when submitted.';
                if (submit) submit.disabled = false;
            }
        }
        [doctor, appointmentDate, appointmentTime].forEach(function (control) {
            control.addEventListener('change', refreshAvailability);
        });
        refreshAvailability();
        window.setInterval(function () {
            if (!document.hidden) refreshAvailability();
        }, 15000);
    });
});
"""


def smtp_settings_cipher(encrypted_settings=None):
    key_path = DATABASE_PATH.with_name(".gapcc_smtp_settings_key")
    environment_key = os.getenv("GAPCC_SMTP_SETTINGS_KEY")
    file_key = None
    if key_path.exists():
        try:
            file_key = key_path.read_text(encoding="ascii").strip()
        except OSError as error:
            raise ValueError("The saved SMTP encryption key could not be read.") from error

    if encrypted_settings is None:
        with connect_db() as connection:
            encrypted_settings = connection.execute(
                "SELECT setting_key, encrypted_value FROM smtp_settings"
            ).fetchall()

    candidates = []
    for source, encryption_key in (
        ("workspace", file_key),
        ("environment", environment_key),
    ):
        if encryption_key and all(key != encryption_key for _, key in candidates):
            candidates.append((source, encryption_key))

    selected_key = None
    selected_source = None
    if encrypted_settings:
        for source, encryption_key in candidates:
            try:
                candidate_cipher = Fernet(encryption_key.encode("ascii"))
                for row in encrypted_settings:
                    candidate_cipher.decrypt(row["encrypted_value"].encode("ascii"))
            except (InvalidToken, ValueError, UnicodeEncodeError):
                continue
            selected_key = encryption_key
            selected_source = source
            break
        if selected_key is None:
            raise ValueError("Available SMTP encryption keys cannot decrypt the saved settings.")
    elif environment_key:
        selected_key = environment_key
        selected_source = "environment"
    elif file_key:
        selected_key = file_key
        selected_source = "workspace"
    else:
        selected_key = Fernet.generate_key().decode("ascii")
        selected_source = "generated"

    try:
        cipher = Fernet(selected_key.encode("ascii"))
    except (ValueError, UnicodeEncodeError) as error:
        raise ValueError("The SMTP settings encryption key is invalid.") from error

    if selected_source == "generated":
        try:
            with key_path.open("x", encoding="ascii") as key_file:
                key_file.write(selected_key)
        except FileExistsError:
            try:
                selected_key = key_path.read_text(encoding="ascii").strip()
                cipher = Fernet(selected_key.encode("ascii"))
            except (OSError, ValueError, UnicodeEncodeError) as error:
                raise ValueError("The saved SMTP encryption key could not be read.") from error
        except OSError as error:
            raise ValueError("The SMTP settings encryption key could not be saved.") from error
    elif selected_source == "environment" and file_key != selected_key:
        try:
            key_path.write_text(selected_key, encoding="ascii")
        except OSError as error:
            raise ValueError("The SMTP settings encryption key could not be saved.") from error
    return cipher


def requeue_interrupted_email_notifications():
    with connect_db() as connection:
        appointment_messages = connection.execute(
            """UPDATE appointment_messages SET status = 'Pending'
            WHERE channel = 'Email' AND status = 'Queued'"""
        ).rowcount
        result_notifications = connection.execute(
            """UPDATE lab_result_notifications SET status = 'Pending'
            WHERE status = 'Queued'"""
        ).rowcount
    return appointment_messages + result_notifications


def load_smtp_settings():
    settings = {}
    with connect_db() as connection:
        encrypted_settings = connection.execute(
            "SELECT setting_key, encrypted_value FROM smtp_settings"
        ).fetchall()
    if encrypted_settings:
        cipher = smtp_settings_cipher(encrypted_settings)
        try:
            settings = {
                row["setting_key"]: cipher.decrypt(row["encrypted_value"].encode("ascii")).decode("utf-8")
                for row in encrypted_settings
            }
        except (InvalidToken, UnicodeDecodeError) as error:
            raise ValueError("Saved SMTP settings could not be decrypted.") from error
    return settings


def send_email_message(destination, subject, body):
    try:
        saved_settings = load_smtp_settings()
    except (sqlite3.Error, ValueError) as error:
        if not os.getenv("GAPCC_SMTP_USER") or not os.getenv("GAPCC_SMTP_PASSWORD"):
            logging.getLogger(__name__).error("SMTP configuration could not be loaded (%s).", type(error).__name__)
            return "Failed"
        logging.getLogger(__name__).warning(
            "Saved SMTP settings could not be loaded; using complete SMTP environment credentials (%s).",
            type(error).__name__,
        )
        saved_settings = {}
    smtp_host = os.getenv("GAPCC_SMTP_HOST") or saved_settings.get("host") or "smtp.gmail.com"
    smtp_user = os.getenv("GAPCC_SMTP_USER") or saved_settings.get("user")
    smtp_password = os.getenv("GAPCC_SMTP_PASSWORD") or saved_settings.get("password")
    try:
        smtp_port = int(os.getenv("GAPCC_SMTP_PORT") or saved_settings.get("port") or "587")
    except (TypeError, ValueError):
        logging.getLogger(__name__).warning("SMTP delivery rejected an invalid port setting.")
        return "Failed"
    smtp_from = os.getenv("GAPCC_SMTP_FROM") or saved_settings.get("from") or smtp_user or ""
    if (
        not smtp_host
        or not smtp_user
        or not smtp_password
        or not smtp_from
        or not 1 <= smtp_port <= 65535
    ):
        return "Not Configured"
    email_pattern = r"[^@\s]+@[^@\s]+\.[^@\s]+"
    if not re.fullmatch(email_pattern, destination.strip()) or not re.fullmatch(email_pattern, smtp_from):
        logging.getLogger(__name__).warning("SMTP delivery rejected an invalid email address.")
        return "Failed"
    message = EmailMessage()
    message["From"] = smtp_from
    message["To"] = destination
    message["Subject"] = subject
    message.set_content(body)
    delivery_stage = "connecting"
    try:
        if smtp_port == 465:
            smtp_connection = smtplib.SMTP_SSL(
                smtp_host,
                smtp_port,
                timeout=15,
                context=ssl.create_default_context(),
            )
        else:
            smtp_connection = smtplib.SMTP(smtp_host, smtp_port, timeout=15)
        with smtp_connection as smtp:
            if smtp_port != 465:
                delivery_stage = "TLS negotiation"
                smtp.ehlo()
                smtp.starttls(context=ssl.create_default_context())
                smtp.ehlo()
            delivery_stage = "authentication"
            smtp.login(smtp_user, smtp_password)
            delivery_stage = "message submission"
            smtp.send_message(message)
    except smtplib.SMTPAuthenticationError as error:
        logging.getLogger(__name__).warning(
            "SMTP authentication was rejected by %s:%s (SMTP status %s). "
            "Check the username, password, and provider app-password requirements.",
            smtp_host,
            smtp_port,
            error.smtp_code,
        )
        return "Failed"
    except (OSError, smtplib.SMTPException) as error:
        if (
            delivery_stage == "authentication"
            and isinstance(error, smtplib.SMTPServerDisconnected)
        ):
            logging.getLogger(__name__).warning(
                "SMTP server disconnected during authentication via %s:%s. "
                "Check the username, password, and provider app-password requirements.",
                smtp_host,
                smtp_port,
            )
        else:
            logging.getLogger(__name__).warning(
                "SMTP delivery failed during %s via %s:%s (%s).",
                delivery_stage,
                smtp_host,
                smtp_port,
                type(error).__name__,
            )
        return "Failed"
    return "Sent"


def send_lab_result_notification(destination):
    return send_email_message(
        destination,
        "GAPCC laboratory result available",
        "A new laboratory or diagnostic result is available in your GAPCC patient results portal. "
        "For your privacy, the result details and document are not included in this email. "
        "Visit the GAPCC Patient Laboratory Results page at /results/patient and request a "
        "one-time password to your registered email address to view it.",
    )


def dispatch_pending_emails() -> tuple[int, int, str | None]:
    now = utc_now()
    today = datetime.now(timezone.utc).date().isoformat()
    try:
        with connect_db() as connection:
            due_message = connection.execute(
                """SELECT 1
                FROM appointment_messages m
                JOIN bookings b ON b.booking_id = m.booking_id
                WHERE m.channel = 'Email'
                AND m.status IN ('Scheduled', 'Pending', 'Not Configured', 'Failed')
                AND m.scheduled_for <= ?
                AND (
                    m.message_type IN ('Update', 'Approval')
                    OR (b.status IN ('Scheduled', 'Confirmed') AND b.appointment_date >= ?)
                )
                LIMIT 1""",
                (now, today),
            ).fetchone()
            due_result_notification = connection.execute(
                """SELECT 1 FROM lab_result_notifications
                WHERE status IN ('Pending', 'Not Configured', 'Failed')
                AND scheduled_for <= ?
                LIMIT 1""",
                (now,),
            ).fetchone()
    except sqlite3.Error as error:
        logging.getLogger(__name__).error("Email notification queues could not be checked (%s).", type(error).__name__)
        return 0, 0, "Email notification queues could not be checked."
    if due_message is None and due_result_notification is None:
        return 0, 0, None
    try:
        settings = load_smtp_settings()
    except (sqlite3.Error, ValueError) as error:
        if not os.getenv("GAPCC_SMTP_USER") or not os.getenv("GAPCC_SMTP_PASSWORD"):
            logging.getLogger(__name__).error("SMTP configuration could not be loaded (%s).", type(error).__name__)
            return 0, 0, "Saved SMTP settings could not be loaded; no emails were sent."
        logging.getLogger(__name__).warning(
            "Saved SMTP settings could not be loaded; using complete SMTP environment credentials (%s).",
            type(error).__name__,
        )
        settings = {}
    if not (
        (os.getenv("GAPCC_SMTP_USER") or settings.get("user"))
        and (os.getenv("GAPCC_SMTP_PASSWORD") or settings.get("password"))
    ):
        return 0, 0, "SMTP is not configured; no emails were sent."
    try:
        with connect_db() as connection:
            connection.execute("BEGIN IMMEDIATE")
            eligible_appointments = connection.execute(
                """SELECT m.message_id, m.message_type, m.message_body, p.email AS destination
                FROM appointment_messages m
                JOIN bookings b ON b.booking_id = m.booking_id
                JOIN patients p ON p.patient_id = m.patient_id
                WHERE m.channel = 'Email'
                AND m.status IN ('Scheduled', 'Pending', 'Not Configured', 'Failed')
                AND m.scheduled_for <= ?
                AND (
                    m.message_type IN ('Update', 'Approval')
                    OR (b.status IN ('Scheduled', 'Confirmed') AND b.appointment_date >= ?)
                )
                ORDER BY m.scheduled_for, m.message_id""",
                (now, today),
            ).fetchall()
            for message in eligible_appointments:
                connection.execute(
                    """UPDATE appointment_messages
                    SET status = 'Queued', destination = ?
                    WHERE message_id = ? AND status IN ('Scheduled', 'Pending', 'Not Configured', 'Failed')""",
                    (message["destination"], message["message_id"]),
                )
            eligible_results = connection.execute(
                """SELECT n.notification_id, n.destination
                FROM lab_result_notifications n
                WHERE n.status IN ('Pending', 'Not Configured', 'Failed')
                AND n.scheduled_for <= ?
                ORDER BY n.scheduled_for, n.notification_id""",
                (now,),
            ).fetchall()
            for notification in eligible_results:
                connection.execute(
                    """UPDATE lab_result_notifications SET status = 'Queued'
                    WHERE notification_id = ?
                    AND status IN ('Pending', 'Not Configured', 'Failed')""",
                    (notification["notification_id"],),
                )
    except sqlite3.Error as error:
        logging.getLogger(__name__).error("Email notification queues could not be claimed (%s).", type(error).__name__)
        return 0, 0, "Email notification queues could not be claimed."
    if not eligible_appointments and not eligible_results:
        return 0, 0, None

    subjects = {
        "Confirmation": "GAPCC appointment confirmation",
        "Reminder": "GAPCC appointment reminder",
        "Approval": "GAPCC appointment approved and confirmed",
        "Update": "GAPCC appointment update",
    }
    sent = 0
    failed = 0
    for message in eligible_appointments:
        delivery_status = send_email_message(
            message["destination"],
            subjects.get(message["message_type"], "GAPCC appointment notification"),
            message["message_body"],
        )
        sent_at = utc_now() if delivery_status == "Sent" else None
        try:
            with connect_db() as connection:
                connection.execute(
                    """UPDATE appointment_messages SET status = ?, sent_at = ?
                    WHERE message_id = ? AND status = 'Queued'""",
                    (delivery_status, sent_at, message["message_id"]),
                )
        except sqlite3.Error as error:
            logging.getLogger(__name__).error(
                "Appointment email result could not be saved for message %s (%s).",
                message["message_id"],
                type(error).__name__,
            )
        sent += delivery_status == "Sent"
        failed += delivery_status != "Sent"
    for notification in eligible_results:
        delivery_status = send_lab_result_notification(notification["destination"])
        sent_at = utc_now() if delivery_status == "Sent" else None
        try:
            with connect_db() as connection:
                connection.execute(
                    """UPDATE lab_result_notifications SET status = ?, sent_at = ?
                    WHERE notification_id = ? AND status = 'Queued'""",
                    (delivery_status, sent_at, notification["notification_id"]),
                )
        except sqlite3.Error as error:
            logging.getLogger(__name__).error(
                "Lab-result notification result could not be saved for notification %s (%s).",
                notification["notification_id"],
                type(error).__name__,
            )
        sent += delivery_status == "Sent"
        failed += delivery_status != "Sent"
    return sent, failed, None


async def appointment_email_dispatch_loop():
    while True:
        sent, failed, error_message = await asyncio.to_thread(dispatch_pending_emails)
        if error_message:
            logging.getLogger(__name__).warning("Appointment email dispatch skipped: %s", error_message)
        elif sent or failed:
            logging.getLogger(__name__).info(
                "Email dispatch complete: %s accepted by SMTP; %s not sent.",
                sent,
                failed,
            )
        await asyncio.sleep(60)


def queue_appointment_messages(connection, booking_id, patient, token_id, service, facility, appointment_date, appointment_time="Clinic hours: 8:00 AM - 3:00 PM"):
    appointment_label = date.fromisoformat(appointment_date).strftime("%B %d, %Y").replace(" 0", " ")
    confirmation = (
        f"We received your GAPCC appointment request. Token: {token_id}. Service: {service}. "
        f"Facility: {facility}. Date: {appointment_label}. {appointment_time}. "
        "The clinic will send a separate email when your appointment is approved and confirmed."
    )
    reminder = f"GAPCC reminder: your {service} appointment is tomorrow at {facility}. Token: {token_id}. {appointment_time}."
    now = datetime.now(timezone.utc)
    reminder_time = datetime.combine(date_string_to_date(appointment_date), datetime.min.time(), timezone.utc)
    confirmation_status = "Not Configured"
    for message_type, body, scheduled_for in (
        ("Confirmation", confirmation, now),
        ("Reminder", reminder, reminder_time - timedelta(days=1)),
    ):
        status = "Scheduled" if message_type == "Reminder" else "Pending"
        message_cursor = connection.execute(
            "INSERT INTO appointment_messages (booking_id, patient_id, message_type, channel, destination, message_body, scheduled_for, sent_at, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (booking_id, patient["patient_id"], message_type, "Email", patient["email"], body, scheduled_for.isoformat(), None, status, now.isoformat()),
        )
        if message_type == "Confirmation":
            delivery_status = send_email_message(patient["email"], "GAPCC appointment request confirmation", body)
            connection.execute("UPDATE appointment_messages SET status = ?, sent_at = ? WHERE message_id = ?", (delivery_status, now.isoformat() if delivery_status == "Sent" else None, message_cursor.lastrowid))
            confirmation_status = delivery_status
    return confirmation_status


def normalize_philippine_number(value):
    digits = "".join(character for character in value if character.isdigit())
    if digits.startswith("63"):
        digits = digits[2:]
    elif digits.startswith("0"):
        digits = digits[1:]
    if len(digits) != 10 or not digits.startswith("9"):
        return None
    return f"+63{digits}"


ENTRY_PORTAL_CSS = """
body.portal-dashboard {
    min-height: 100vh;
    background: #f1f7f6;
}
body.portal-dashboard > header,
body.portal-dashboard > nav,
body.portal-dashboard > .ribbon-info {
    display: none !important;
}
body.portal-dashboard main {
    width: 100%;
    min-height: 100vh;
    margin: 0;
    padding: 22px 3%;
}
body.entry-shell {
    min-height: 100vh;
    background-color: #395344;
    background-image: linear-gradient(180deg, rgba(12, 31, 31, .08), rgba(10, 28, 25, .42)), url('/assets/portal.jpg');
    background-position: center;
    background-size: cover;
    background-attachment: fixed;
}
body.entry-shell header,
body.entry-shell nav,
body.entry-shell > .ribbon-info {
    display: none !important;
}
body.entry-shell main {
    display: grid;
    place-items: center;
    width: 100%;
    min-height: 100vh;
    margin: 0;
    padding: 36px 5%;
}
body.entry-shell main > .card {
    width: min(560px, 100%);
    margin: 0;
    padding: 42px;
    border: 1px solid rgba(255, 255, 255, .62);
    border-radius: 20px;
    background: rgba(22, 34, 31, .42);
    color: #fff;
    box-shadow: 0 24px 70px rgba(0, 0, 0, .24);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
}
body.entry-shell main > .card h1,
body.entry-shell main > .card h2 {
    color: #fff;
}
body.entry-shell main > .card h1 {
    margin-bottom: 10px;
    font-family: 'Trebuchet MS', Arial, sans-serif;
    font-size: 32px;
}
body.entry-shell main > .card p,
body.entry-shell main > .card .muted {
    color: rgba(255, 255, 255, .88);
}
body.entry-shell main > .card label {
    color: rgba(255, 255, 255, .9);
    text-transform: none;
    font-size: 12px;
}
body.entry-shell main > .card input,
body.entry-shell main > .card select,
body.entry-shell main > .card textarea {
    min-height: 54px;
    border: 1px solid rgba(255, 255, 255, .48);
    border-radius: 11px;
    background: rgba(255, 255, 255, .12);
    color: #fff;
}
body.entry-shell main > .card select option {
    color: #17353a;
}
body.entry-shell main > .card input::placeholder,
body.entry-shell main > .card textarea::placeholder {
    color: rgba(255, 255, 255, .68);
}
body.entry-shell main > .card input:focus-visible,
body.entry-shell main > .card select:focus-visible,
body.entry-shell main > .card textarea:focus-visible,
body.entry-shell main > .card .button:focus-visible {
    outline: 2px solid #d7e849;
    outline-offset: 3px;
}
body.entry-shell main > .card .button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    min-height: 56px;
    margin-top: 22px;
    border-radius: 11px;
    background: linear-gradient(105deg, #c6d91e 0%, #25b878 100%);
    color: #fff;
    font-size: 16px;
}
body.entry-shell main > .card a:not(.button) {
    color: #fff;
}
body.entry-shell main > .card .entry-signup {
    margin: 12px 0 0;
    color: rgba(255, 255, 255, .88);
    text-align: center;
    font-family: 'Trebuchet MS', Arial, sans-serif;
    font-size: 13px;
}
body.entry-shell main > .card .entry-signup a {
    color: #A7EBF2;
    font-weight: 800;
    text-decoration: none;
}
body.entry-shell main > .card .entry-signup a:hover {
    text-decoration: underline;
}
body.entry-patient main {
    grid-template-rows: 1fr auto;
}
body.entry-patient main > .card {
    align-self: center;
}
body.entry-patient main > .portal-login-links {
    display: flex;
    flex-wrap: wrap;
    justify-content: center;
    gap: 12px 24px;
    margin: 18px 0 0;
    padding: 13px 22px;
    border: 1px solid rgba(255, 255, 255, .35);
    border-radius: 14px;
    background: rgba(22, 34, 31, .5);
    text-align: center;
}
body.entry-patient main > .portal-login-links a {
    color: #A7EBF2;
    font-family: 'Trebuchet MS', Arial, sans-serif;
    font-size: 13px;
    font-weight: 800;
    text-decoration: none;
}
body.entry-patient main > .portal-login-links a:hover {
    text-decoration: underline;
}
body.entry-shell main > .card .entry-home {
    display: block;
    margin-top: 16px;
    text-align: center;
    font-family: 'Trebuchet MS', Arial, sans-serif;
    font-size: 13px;
}
body.entry-shell main > .card .notice {
    border-left-color: #d7e849;
    background: rgba(255, 255, 255, .13);
    color: #fff;
}
body.entry-booking main > .card {
    width: min(900px, 100%);
}
body.entry-booking-required main > .card {
    width: min(560px, 100%);
}
body.entry-admin main > .card .grid,
body.entry-doctor main > .card .grid {
    grid-template-columns: 1fr;
}
body.entry-booking main > .card .grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
}
@media (max-width: 650px) {
    body.entry-shell {
        background-attachment: scroll;
    }
    body.entry-shell main {
        padding: 20px 16px;
    }
    body.entry-shell main > .card {
        padding: 28px 22px;
    }
    body.entry-booking main > .card .grid {
        grid-template-columns: 1fr;
    }
}
body.entry-admin,
body.entry-doctor {
    background-color: #011C40;
    background-image: linear-gradient(180deg, rgba(38, 101, 140, .18), rgba(1, 28, 64, .72)), url('/assets/portal.jpg');
}
body.entry-admin main > .card,
body.entry-doctor main > .card {
    background: rgba(1, 28, 64, .48);
    border-color: rgba(167, 235, 242, .72);
}
body.entry-admin main > .card .button,
body.entry-doctor main > .card .button {
    background: linear-gradient(105deg, #54ACBF 0%, #26658C 100%);
}
body.entry-admin main > .card .notice,
body.entry-doctor main > .card .notice {
    border-left-color: #54ACBF;
}
body.entry-admin main > .card input:focus-visible,
body.entry-admin main > .card select:focus-visible,
body.entry-admin main > .card .button:focus-visible,
body.entry-doctor main > .card input:focus-visible,
body.entry-doctor main > .card select:focus-visible,
body.entry-doctor main > .card .button:focus-visible {
    outline-color: #A7EBF2;
}
"""


PORTAL_VIEW_SCRIPT = """<style>
.gapcc-view-hidden { display:none !important; }
.doctor-nav a.is-active { background:#e9f6f3; color:#087e7a; box-shadow:inset 3px 0 0 #087e7a; }
.gapcc-live { position:fixed; right:18px; bottom:16px; z-index:50; display:flex; align-items:center; gap:7px; padding:7px 12px; border:1px solid #cfe3e6; border-radius:999px; background:#fff; color:#3d5a60; font:700 11px/1 system-ui,sans-serif; box-shadow:0 4px 14px rgba(16,60,70,.12); cursor:pointer; }
.gapcc-live i { width:8px; height:8px; border-radius:50%; background:#16a34a; box-shadow:0 0 0 3px rgba(22,163,74,.18); }
.gapcc-live.is-paused i { background:#d97706; box-shadow:0 0 0 3px rgba(217,119,6,.18); }
.gapcc-live.is-busy i { animation:gapcc-pulse .8s ease-in-out infinite; }
@keyframes gapcc-pulse { 50% { opacity:.35; } }
.admin-content > *, .doctor-dashboard-main > * { animation:gapcc-fade .25s ease-out; }
@keyframes gapcc-fade { from { opacity:0; transform:translateY(4px); } to { opacity:1; transform:none; } }
</style><script>
window.addEventListener('load', function () {
    var admin = document.querySelector('.admin-dashboard');
    var doctor = document.querySelector('.doctor-dashboard');
    if (!admin && !doctor) return;
    var navSelector = admin ? '.admin-sidebar-nav a[href^="#"]' : '.doctor-nav a[href^="#"]';
    var extras = admin ? { 'admin-overview': ['.analytics-grid', '#admin-dashboard-counts'] } : { 'doctor-overview': ['.doctor-stats'] };
    var loginUrl = admin ? '/admin' : '/doctor';
    var countSelector = admin ? '#admin-dashboard-counts a[href^="#"]' : '#doctor-dashboard-counts a[href^="#"]';
    var views = {};
    var order = [];
    var liveSelectors = [];
    function collect() {
        views = {}; order = []; liveSelectors = [];
        document.querySelectorAll(navSelector).forEach(function (link) {
            var id = link.getAttribute('href').slice(1);
            var target = document.getElementById(id);
            if (!target) return;
            if (target.closest('aside')) { link.classList.add('gapcc-view-hidden'); return; }
            var items = [target];
            (extras[id] || []).forEach(function (selector) {
                var extra = document.querySelector(selector);
                if (extra) items.push(extra);
            });
            views[id] = items;
            order.push(id);
            items.forEach(function (item) { liveSelectors.push(item.id ? '#' + item.id : '.' + item.className.split(' ')[0]); });
        });
        liveSelectors.push(admin ? '#admin-dashboard-counts' : '#doctor-dashboard-counts');
    }
    function show(id, push) {
        if (!views[id]) return;
        order.forEach(function (viewId) {
            views[viewId].forEach(function (item) { item.classList.toggle('gapcc-view-hidden', viewId !== id); });
        });
        document.querySelectorAll(navSelector).forEach(function (link) {
            var active = link.getAttribute('href') === '#' + id;
            link.classList.toggle('is-active', active);
            if (active) link.setAttribute('aria-current', 'location'); else link.removeAttribute('aria-current');
            if (active && doctor) {
                var crumb = document.querySelector('.doctor-topbar p');
                if (crumb) crumb.textContent = 'Clinical workspace / ' + link.textContent.trim();
            }
        });
        if (push) history.replaceState(null, '', '#' + id);
        window.scrollTo({ top: 0 });
    }
    function current() {
        var hash = location.hash.slice(1);
        return views[hash] ? hash : order[0];
    }
    collect();
    document.addEventListener('click', function (event) {
        var link = event.target.closest(navSelector + ', ' + countSelector);
        if (!link) return;
        var id = link.getAttribute('href').slice(1);
        if (!views[id]) return;
        event.preventDefault();
        show(id, true);
    });
    window.addEventListener('hashchange', function () { show(current(), false); });
    show(current(), false);

    var pill = document.createElement('button');
    pill.type = 'button';
    pill.className = 'gapcc-live';
    pill.title = 'Data refreshes automatically. Click to refresh now.';
    pill.innerHTML = '<i></i><span>Live</span>';
    document.body.appendChild(pill);
    var label = pill.querySelector('span');
    function isBusy(node) {
        if (node.querySelector('dialog[open]')) return true;
        var active = document.activeElement;
        if (active && node.contains(active) && /^(INPUT|TEXTAREA|SELECT)$/.test(active.tagName)) return true;
        return Array.prototype.some.call(node.querySelectorAll('input:not([type=hidden]), textarea, select'), function (field) {
            if (field.tagName === 'SELECT') return Array.prototype.some.call(field.options, function (o) { return o.selected !== o.defaultSelected; });
            return field.value !== field.defaultValue;
        });
    }
    var running = false;
    function refresh() {
        if (running) return;
        var form = document.querySelector(admin ? '#admin-search' : '.doctor-search');
        if (!form) return;
        running = true;
        pill.classList.add('is-busy');
        var credentials = new URLSearchParams({ email: form.querySelector('[name=email]').value, password: form.querySelector('[name=password]').value });
        fetch(loginUrl, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: credentials })
            .then(function (response) { return response.text(); })
            .then(function (text) {
                var fresh = new DOMParser().parseFromString(text, 'text/html');
                if (!fresh.querySelector(admin ? '.admin-dashboard' : '.doctor-dashboard')) throw new Error('paused');
                liveSelectors.forEach(function (selector) {
                    var oldNode = document.querySelector(selector);
                    var newNode = fresh.querySelector(selector);
                    if (!oldNode || !newNode || isBusy(oldNode)) return;
                    newNode.classList.toggle('gapcc-view-hidden', oldNode.classList.contains('gapcc-view-hidden'));
                    oldNode.replaceWith(newNode);
                });
                collect();
                show(current(), false);
                if (window.gapccReapplyFilters) window.gapccReapplyFilters();
                pill.classList.remove('is-paused');
                label.textContent = 'Live · updated ' + new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            })
            .catch(function () {
                pill.classList.add('is-paused');
                label.textContent = 'Auto-refresh paused';
            })
            .finally(function () { running = false; pill.classList.remove('is-busy'); });
    }
    pill.addEventListener('click', refresh);
    setInterval(function () { if (!document.hidden) refresh(); }, 30000);
    document.addEventListener('visibilitychange', function () { if (!document.hidden) refresh(); });
});
</script>"""


def page(title, body, page_class="", profile_html=""):
    if not page_class and title in {"Admin Console", "Doctor Workspace"}:
        page_class = "portal-dashboard"
    if title == "Doctor Workspace":
        profile_html = profile_html.replace("Registration number", "License number")
        body = body.replace("Registration Number", "License Number").replace("Registration #", "License Number")
        body = body.replace(
            '<a class="doctor-home-link" href="/">Return to GAPCC Home</a>',
            '<a class="doctor-home-link sidebar-logout" href="/doctor/logout" style="color:#b42318;font-weight:800">&#x23FB; Log Out</a><a class="doctor-home-link" href="/" style="margin-top:0;border-top:0">Switch Portal</a>',
        )
        resident_password_field = '<input type="password" name="password" required></div><div><label>Specialization</label>'
        resident_password_field_with_policy = f'<input type="password" name="password" minlength="12" pattern="{PASSWORD_POLICY_PATTERN}" autocomplete="new-password" required><small>{PASSWORD_POLICY_MESSAGE}</small></div><div><label>Specialization</label>'
        body = body.replace(resident_password_field, resident_password_field_with_policy, 1)
        profile_html = """<script>document.addEventListener('DOMContentLoaded',function(){const sidebar=document.querySelector('.doctor-sidebar');const navigation=sidebar&&sidebar.querySelector('.doctor-nav');const summary=sidebar&&sidebar.querySelector('.doctor-sidebar-profile');const panel=document.querySelector('.doctor-profile-panel');const profile=panel&&panel.querySelector('.portal-profile');const countBar=document.getElementById('doctor-dashboard-counts');const topbar=document.querySelector('.doctor-topbar');if(sidebar&&navigation&&panel&&profile){profile.id='doctor-profile';sidebar.insertBefore(profile,navigation);if(summary)summary.remove();panel.remove();}if(countBar&&topbar)topbar.appendChild(countBar);});</script>""" + profile_html
    if title in {"Admin Console", "Doctor Workspace"}:
        body += PORTAL_VIEW_SCRIPT
    body = profile_html + """<script>
    const doctorRoster = [
        { name: 'Dr. April Michelle C.', specialization: 'General Physician', availability: 'Available Today', schedule: 'Fri: 7:00 AM - 4:00 PM', contact: '0917-123-4567', branch: 'San Pedro' },
        { name: 'Dr. Maria Santos', specialization: 'Cardiology', availability: 'Morning Slots', schedule: 'Tue: 8:00 AM - 11:00 AM', contact: '0917-456-7890', branch: 'San Pedro' },
        { name: 'Dr. Leonardo Cruz', specialization: 'Pediatrics', availability: 'Available Today', schedule: 'Sat: 11:00 AM - 2:00 PM', contact: '0917-222-3344', branch: 'Narra' },
        { name: 'Dr. Evelyn Reyes', specialization: 'Dermatology', availability: 'Afternoon Slots', schedule: 'Wed: 2:00 PM - 5:00 PM', contact: '0917-445-7891', branch: 'San Pedro' },
        { name: 'Dr. Paulo Nolasco', specialization: 'Neurology', availability: 'Morning Slots', schedule: 'Mon: 9:00 AM - 1:00 PM', contact: '0917-901-3311', branch: 'Biñan' },
        { name: 'Dr. Fiona Dela Vega', specialization: 'Family Medicine', availability: 'Available Today', schedule: 'Sun: 9:00 AM - 12:00 PM', contact: '0917-770-2255', branch: 'San Pedro' }
    ];
    const doctorSpecializations = ['All', 'General Physician', 'Cardiology', 'Pediatrics', 'Dermatology', 'Family Medicine', 'Neurology'];
    const doctorAvailability = ['All', 'Available Today', 'Morning Slots', 'Afternoon Slots'];
    let activeSpecialization = 'All';
    let activeAvailability = 'All';

    function showRibbonInfo(title, text) {
        const panel = document.getElementById('ribbon-info');
        if (!panel) return;
        panel.classList.add('visible');
        panel.innerHTML = '<h2>' + title + '</h2><p>' + text + '</p>';
    }

    function showDoctorFinder() {
        const panel = document.getElementById('ribbon-info');
        if (!panel) return;
        panel.classList.add('visible');

        const specializationButtons = doctorSpecializations.map(function (spec) {
            const activeClass = spec === activeSpecialization ? 'active' : '';
            return '<button class="doctor-chip ' + activeClass + '" data-specialization="' + spec + '">' + spec + '</button>';
        }).join('');

        const availabilityButtons = doctorAvailability.map(function (state) {
            const activeClass = state === activeAvailability ? 'active' : '';
            return '<button class="doctor-chip ' + activeClass + '" data-availability="' + state + '">' + state + '</button>';
        }).join('');

        const filteredDoctors = doctorRoster.filter(function (doctor) {
            const matchesSpecialization = activeSpecialization === 'All' || doctor.specialization === activeSpecialization;
            const matchesAvailability = activeAvailability === 'All' || doctor.availability === activeAvailability;
            return matchesSpecialization && matchesAvailability;
        });

        const rows = filteredDoctors.length ? filteredDoctors.map(function (doctor) {
            return '<div class="doctor-row"><div class="doctor-profile"><strong>' + doctor.name + '</strong><span>' + doctor.specialization + '</span></div><div class="doctor-meta"><span class="status-pill">' + doctor.availability + '</span><small>' + doctor.schedule + '</small></div><div class="doctor-contact"><strong>' + doctor.contact + '</strong><small>' + doctor.branch + '</small></div></div>';
        }).join('') : '<div class="doctor-row empty"><strong>No doctors match the selected filters.</strong></div>';

        panel.innerHTML = '<div class="doctor-finder"><div class="doctor-panel left"><h2>Find a Doctor</h2><p class="finder-label">Filter by Specialization</p><div class="doctor-chips">' + specializationButtons + '</div><p class="finder-label">Filter by Availability</p><div class="doctor-chips">' + availabilityButtons + '</div></div><div class="doctor-panel right"><div class="doctor-toolbar"><strong>Available Doctors</strong><button class="reset-btn" onclick="activeSpecialization=\'All\'; activeAvailability=\'All\'; showDoctorFinder();">Reset</button></div><div class="doctor-list">' + rows + '</div></div></div>';

        document.querySelectorAll('.doctor-chip[data-specialization]').forEach(function (button) {
            button.addEventListener('click', function () {
                activeSpecialization = button.dataset.specialization;
                showDoctorFinder();
            });
        });

        document.querySelectorAll('.doctor-chip[data-availability]').forEach(function (button) {
            button.addEventListener('click', function () {
                activeAvailability = button.dataset.availability;
                showDoctorFinder();
            });
        });
    }

    setInterval(function () {
        const laboratoryPackages = document.getElementById('laboratory-packages');
        if (laboratoryPackages) {
            const packageTitle = laboratoryPackages.querySelector('h2');
            if (packageTitle) packageTitle.textContent = 'Featured Tests';
            laboratoryPackages.querySelector('.lab-layout')?.style.setProperty('display', 'block', 'important');
            laboratoryPackages.querySelector('.lab-photo')?.style.setProperty('display', 'none', 'important');
            const groups = laboratoryPackages.querySelector('.lab-groups');
        }
        const testPackagesLink = Array.from(document.querySelectorAll('nav a')).find(function (link) { return link.textContent.trim() === 'Test packages'; });
        const featuredTestLink = Array.from(document.querySelectorAll('nav a')).find(function (link) { return link.textContent.trim() === 'Featured test'; });
        const packageButtons = document.querySelector('#laboratory-packages .package-options');
        const detailedGroups = document.querySelector('#laboratory-packages .lab-groups');
        const managedDetails = document.querySelectorAll('#laboratory-packages .managed-package-details');
        function showPackageView(showDetails, event) {
            event.preventDefault();
            if (packageButtons) packageButtons.style.display = showDetails ? 'none' : 'grid';
            if (detailedGroups) detailedGroups.style.display = showDetails ? 'grid' : 'none';
            managedDetails.forEach(function (details) { details.style.display = 'none'; });
            document.getElementById('laboratory-packages').style.display = 'block';
            document.getElementById('laboratory-packages').scrollIntoView({behavior:'smooth'});
        }
        if (testPackagesLink) testPackagesLink.onclick = showPackageView.bind(null, false);
        if (featuredTestLink) featuredTestLink.onclick = showPackageView.bind(null, true);
        document.querySelectorAll('nav a').forEach(function (link) {
            const label = link.textContent.trim();
            if (label === 'Test packages' || label === 'Featured test') {
                link.onclick = function (event) {
                    showPackageView(label === 'Featured test', event);
                };
            }
        });
        if (packageButtons) packageButtons.style.display = 'grid';
        if (detailedGroups) detailedGroups.style.display = 'none';
        managedDetails.forEach(function (details) { details.style.display = 'none'; });
        document.querySelectorAll('#operating-hours .hours-card').forEach(function (card) {
            const heading = card.querySelector('h3');
            const image = card.querySelector('img');
            if (heading && image && heading.textContent.trim() === 'Drug Test') {
                    image.src = '/assets/810319289_39758333547099033_8912087341618590151_n.jpg';
                image.alt = 'Drug screening rapid test kits';
            }
        });
    }, 250);
    document.addEventListener("DOMContentLoaded", function () {
        const laboratoryPackages = document.getElementById('laboratory-packages');
        if (laboratoryPackages) {
            const packageTitle = laboratoryPackages.querySelector('h2');
            if (packageTitle) packageTitle.textContent = 'Featured Tests';
            const packageStyle = document.createElement('style');
            packageStyle.textContent = '#laboratory-packages .facility-copy { background:#f7faf9 !important; } #laboratory-packages .lab-layout { display:block !important; } #laboratory-packages .lab-photo { display:none; } #laboratory-packages .lab-groups { display:grid !important; grid-template-columns:repeat(3,minmax(0,1fr)) !important; gap:28px 54px !important; margin-top:24px; } #laboratory-packages .lab-group { padding:0 0 12px !important; background:transparent !important; border:0 !important; } #laboratory-packages .lab-group h3 { display:inline-flex; min-width:150px; margin:0 0 12px !important; padding:9px 14px; border-radius:5px; color:#fff !important; background:#18506b; font-size:14px !important; } #laboratory-packages .lab-group:nth-child(2) h3 { background:#a45176; } #laboratory-packages .lab-group:nth-child(3) h3 { background:#e29b09; } #laboratory-packages .lab-group:nth-child(4) h3 { background:#2ba6bb; } #laboratory-packages .lab-group:nth-child(5) h3 { background:#17a879; } #laboratory-packages .lab-group p { color:#334b53 !important; font-size:12px !important; line-height:1.8 !important; } @media(max-width:800px) { #laboratory-packages .lab-groups { grid-template-columns:repeat(2,minmax(0,1fr)) !important; } } @media(max-width:520px) { #laboratory-packages .lab-groups { grid-template-columns:1fr !important; } }';
            document.head.appendChild(packageStyle);
        }
        const drugTestObserver = new MutationObserver(function () {
            const drugTestImage = document.querySelector('#operating-hours img[alt="Drug test laboratory equipment"]');
            if (drugTestImage) {
                drugTestImage.src = '/assets/810319289_39758333547099033_8912087341618590151_n.jpg';
                drugTestImage.alt = 'Drug screening rapid test kits';
            }
        });
        drugTestObserver.observe(document.body, {childList: true, subtree: true});
        document.querySelectorAll('#laboratory-packages [data-package-key]').forEach(function (button) {
            button.addEventListener('click', function () {
                const details = document.getElementById(button.getAttribute('aria-controls'));
                if (!details) return;
                const expanded = button.getAttribute('aria-expanded') === 'true';
                button.setAttribute('aria-expanded', String(!expanded));
                details.hidden = expanded;
            });
        });
        setInterval(function () {
            document.querySelectorAll('#operating-hours .hours-card').forEach(function (card) {
                const heading = card.querySelector('h3');
                const image = card.querySelector('img');
                if (heading && image && heading.textContent.trim() === 'Drug Test') {
                    image.src = '/assets/810319289_39758333547099033_8912087341618590151_n.jpg';
                    image.alt = 'Drug screening rapid test kits';
                }
            });
        }, 100);
        setTimeout(function () {
            const xrayImage = document.querySelector('#operating-hours img[alt="X-ray imaging equipment"]');
            if (xrayImage) {
                xrayImage.src = 'https://images.unsplash.com/photo-1530497610245-94d3c16c777d?auto=format&fit=crop&w=900&q=90';
                xrayImage.alt = 'Radiology X-ray imaging equipment';
            }
            const drugTestImage = document.querySelector('#operating-hours img[alt="Drug test laboratory equipment"]');
            if (drugTestImage) {
                    drugTestImage.src = '/assets/810319289_39758333547099033_8912087341618590151_n.jpg';
                drugTestImage.alt = 'Drug screening rapid test kits';
            }
            const emergencyImage = document.querySelector('#operating-hours img[alt="Emergency care team in hospital"]');
            if (emergencyImage) {
                emergencyImage.src = 'https://images.unsplash.com/photo-1516841273335-e39b37888115?auto=format&fit=crop&w=900&q=90';
                emergencyImage.alt = 'Emergency trauma care team in hospital';
            }
        }, 0);
        const homeLink = document.querySelector("nav .nav-item > a[href='/']");
        if (homeLink) {
            homeLink.onclick = function () {
                window.location.href = "/";
            };
        }
        document.querySelectorAll("nav .dropdown a").forEach(function (link) {
            if (link.textContent.trim() === "Patient" && link.closest(".nav-item").querySelector("a").textContent.trim() === "Results") {
                link.href = "/portal";
                link.onclick = null;
            }
        });

        document.querySelectorAll('.action-tile').forEach(function (tile) {
            tile.addEventListener('click', function () {
                document.querySelectorAll('.action-tile').forEach(function (item) {
                    item.classList.remove('active');
                });
                tile.classList.add('active');
            });
        });

        document.querySelectorAll('[data-panel="doctor-finder"]').forEach(function (link) {
            link.addEventListener('click', function (event) {
                event.preventDefault();
                showDoctorFinder();
            });
        });
    });
    </script>""" + body
    return HTMLResponse(
        f"""<!doctype html><html lang="en"><head>
        <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{escape(title)} | GAPCC</title>
        <style>
        :root {{ --blue:#07566a; --cyan:#12c7d1; --teal:#078b83; --bg:#f3f8fa; --line:#d9e8eb; --shadow:rgba(8,50,76,.12); --card:#ffffff; --accent:#0c9bb5; --accent-2:#0e7d87; --soft:#eaf9fb; --deep:#053d4f; }}
        * {{ box-sizing:border-box; }} html {{ overflow-x:hidden; }} body {{ margin:0; min-width:320px; background:linear-gradient(180deg, #edf7fa 0%, #f6fbfc 100%); color:#1e293b; font-family:Segoe UI,Arial,sans-serif; }}
        header {{ display:flex; justify-content:space-between; align-items:center; gap:20px; padding:16px 7%; background:rgba(255,255,255,.96); backdrop-filter:blur(8px); border-bottom:1px solid var(--line); box-shadow:0 2px 16px rgba(14,55,68,.04); }} header > a:first-child {{ min-width:0; }}
        header a {{ color:var(--blue); font-size:24px; font-weight:700; text-decoration:none; }} header span {{ color:var(--cyan); }} header small {{ display:block; margin-top:3px; color:#64748b; font-size:10px; letter-spacing:.5px; }}
        nav {{ display:grid; grid-template-columns:repeat(6, minmax(0, 1fr)); background:linear-gradient(90deg, var(--cyan), #1fc7d2 35%, #09bac9 100%); border-bottom:4px solid var(--blue); box-shadow:0 8px 20px rgba(11,120,140,.12); }} .nav-item {{ position:relative; min-width:0; }} .nav-item > a {{ display:flex; min-height:60px; align-items:center; justify-content:center; padding:12px 8px; color:white; text-align:center; text-decoration:none; font-size:12px; font-weight:700; text-transform:uppercase; border-right:1px solid rgba(255,255,255,.25); transition:background .2s, box-shadow .2s, transform .2s; }} .nav-item:hover > a, .nav-item:focus-within > a {{ background:rgba(255,255,255,.14); box-shadow:inset 0 -4px 0 white; transform:translateY(-1px); }} .dropdown {{ position:absolute; z-index:10; top:100%; left:0; display:none; min-width:230px; padding:5px 0; background:white; border:1px solid #dbe4ec; border-top:3px solid var(--blue); box-shadow:0 10px 24px rgba(3,67,117,.18); }} .nav-item:hover .dropdown, .nav-item:focus-within .dropdown {{ display:block; }} .dropdown a {{ display:block; padding:11px 15px; color:#334155; text-decoration:none; font-size:13px; font-weight:600; white-space:normal; }} .dropdown a:hover, .dropdown a:focus-visible {{ background:#eef8fa; color:var(--blue); outline:none; }}
        .hero {{ width:100%; min-height:430px; display:flex; align-items:center; justify-content:center; padding:44px 5%; background:linear-gradient(135deg, rgba(5,61,79,.45), rgba(11,147,160,.24)), url('https://images.unsplash.com/photo-1576091160399-112ba8d25d1d?auto=format&fit=crop&w=1800&q=85') center/cover no-repeat; color:white; text-align:center; }} .hero-copy {{ max-width:820px; padding:28px 42px; background:rgba(8,74,93,.18); border:1px solid rgba(255,255,255,.18); border-radius:18px; backdrop-filter:blur(2px); box-shadow:0 18px 35px rgba(0,0,0,.12); }} .hero-copy h1 {{ margin:0 0 8px; color:white; text-shadow:0 2px 12px rgba(0,0,0,.6); font-size:clamp(2.2rem, 4vw, 4.4rem); letter-spacing:-.04em; }} .hero-copy p {{ margin:0; color:white; opacity:1; text-shadow:0 2px 8px rgba(0,0,0,.5); font-size:1.08rem; }}
        main {{ width:100%; margin:34px auto; padding:0 4%; }} .card {{ width:min(1200px,100%); margin:0 auto 24px; padding:28px; background:var(--card); border:1px solid #e2e8f0; border-radius:14px; box-shadow:0 10px 28px var(--shadow); }}
        .action-grid {{ width:min(1200px,100%); display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin:0 auto 24px; }} .action-tile {{ min-height:92px; padding:18px; border:0; border-radius:10px; background:linear-gradient(135deg, #07566a 0%, #0d7790 100%); color:white; text-align:left; box-shadow:0 8px 18px rgba(10,82,105,.2); text-decoration:none; transition:all .2s ease; }} .action-tile:hover, .action-tile:focus-visible {{ background:linear-gradient(135deg, #0d6880 0%, #0ea7b6 100%); transform:translateY(-2px); box-shadow:0 10px 22px rgba(10,82,105,.28); outline:none; }} .action-tile:active, .action-tile.active {{ background:linear-gradient(135deg, #0d8f9f 0%, #13b2c2 100%); transform:translateY(0); box-shadow:0 4px 12px rgba(10,82,105,.22); }} .action-tile strong {{ display:block; font-size:14px; }} .action-tile small {{ display:block; margin-top:7px; color:#d8f6f8; font-size:11px; }}
        .stats {{ width:min(1200px,100%); display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:16px; margin:0 auto 24px; }} .stat-card {{ background:linear-gradient(180deg, #ffffff, #f5fbfd); border:1px solid #dfeaf0; border-radius:12px; padding:18px 20px; box-shadow:0 8px 18px rgba(8,26,34,.04); }} .stat-card .label {{ display:block; color:#64748b; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.1em; }} .stat-card .value {{ display:block; margin-top:10px; color:var(--deep); font-size:clamp(1.4rem, 2vw, 2.2rem); font-weight:800; }} .stat-card .note {{ display:block; margin-top:8px; color:#4b5563; font-size:12px; }}
        .feature-strip {{ width:min(1200px,100%); margin:0 auto 24px; display:grid; grid-template-columns:repeat(3, minmax(0,1fr)); gap:14px; }} .feature-card {{ display:flex; align-items:center; gap:14px; background:linear-gradient(180deg, #ffffff, #f8fcfe); border:1px solid #dfeaf0; border-radius:12px; padding:18px; box-shadow:0 8px 18px rgba(8,26,34,.04); }} .feature-icon {{ width:46px; height:46px; display:flex; align-items:center; justify-content:center; background:linear-gradient(135deg, #eafcff, #dff9f9); color:var(--blue); border-radius:12px; font-weight:800; }} .feature-card h3 {{ margin:0 0 4px; font-size:15px; color:var(--blue); }} .feature-card p {{ margin:0; font-size:12px; color:#475569; }}
        .facility-section {{ width:min(1200px,100%); margin:0 auto 24px; padding:0; }} .facility-layout {{ display:grid; grid-template-columns:1fr 1fr; overflow:hidden; background:#fff; border:1px solid #dfeaf0; border-radius:14px; box-shadow:0 10px 28px var(--shadow); }} .facility-photo {{ width:100%; height:100%; min-height:340px; object-fit:cover; display:block; }} .facility-copy {{ padding:28px; }} .facility-copy .eyebrow {{ margin:0 0 8px; color:var(--accent-2); font-size:11px; font-weight:800; letter-spacing:.12em; text-transform:uppercase; }} .facility-copy h2 {{ margin:0; color:var(--blue); font-size:28px; }} .facility-copy p {{ color:#526b72; font-size:14px; line-height:1.6; }} .facility-address {{ display:flex; gap:12px; align-items:flex-start; margin:20px 0 12px; padding:15px; background:#effafa; border-left:4px solid var(--cyan); border-radius:8px; color:#214b55; font-size:13px; line-height:1.5; }} .facility-address strong {{ display:block; margin-bottom:3px; color:var(--blue); }} .facility-contact {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:18px; }} .facility-contact a {{ display:block; padding:13px 14px; background:#f7fbfc; border:1px solid #dfeaf0; border-radius:8px; color:var(--blue); font-size:13px; font-weight:800; text-decoration:none; }} .facility-contact a span {{ display:block; margin-bottom:4px; color:#789097; font-size:10px; font-weight:700; letter-spacing:.08em; text-transform:uppercase; }} .facility-contact a:hover {{ border-color:var(--cyan); background:#effafa; }} .facility-actions {{ display:flex; flex-wrap:wrap; gap:10px; }} .map-link {{ display:inline-flex; align-items:center; min-height:42px; padding:0 15px; border-radius:6px; background:var(--blue); color:#fff; font-size:12px; font-weight:800; text-decoration:none; }} .map-link.secondary {{ background:#e5f7f5; color:var(--accent-2); }} .facility-map {{ width:100%; height:280px; margin-top:16px; border:0; border-radius:10px; }}
        h1,h2 {{ margin-top:0; color:var(--blue); }} p {{ line-height:1.55; }} .grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:16px; }} label {{ display:block; margin-bottom:6px; color:#475569; font-size:11px; font-weight:700; text-transform:uppercase; }} input,select {{ width:100%; padding:12px; border:1px solid #cbd5e1; border-radius:6px; background:#f8fafc; font-size:14px; }} textarea {{ width:100%; padding:12px; border:1px solid #cbd5e1; border-radius:6px; background:#f8fafc; font:inherit; resize:vertical; }} .table-scroll {{ overflow-x:auto; margin:0 0 24px; border:1px solid #dfeaf0; border-radius:8px; }} table {{ width:100%; min-width:760px; border-collapse:collapse; background:white; font:13px 'Trebuchet MS', Arial, sans-serif; }} th,td {{ padding:11px 12px; border-bottom:1px solid #e5eef1; text-align:left; vertical-align:top; }} th {{ background:#eef9fb; color:var(--blue); font-size:11px; text-transform:uppercase; letter-spacing:.05em; white-space:nowrap; }} tr:last-child td {{ border-bottom:0; }} td small {{ color:#64748b; }} .phone-field {{ display:flex; align-items:center; }} .phone-field span {{ padding:12px 10px; border:1px solid #cbd5e1; border-right:0; border-radius:6px 0 0 6px; background:#e2e8f0; color:#475569; font-weight:700; }} .phone-field input {{ border-radius:0 6px 6px 0; }}
        .button {{ display:inline-block; margin-top:22px; padding:13px 18px; border:0; border-radius:6px; background:var(--blue); color:white; cursor:pointer; font-size:14px; font-weight:700; text-decoration:none; }} header .button {{ white-space:nowrap; }} .button.green {{ background:var(--teal); }} .notice {{ margin:16px 0; padding:14px; border-left:4px solid var(--cyan); background:#ecfeff; }} .error {{ border-color:#d9383a; background:#fef2f2; }} .muted {{ color:#64748b; font-size:13px; }}
        .ribbon-info {{ display:none; width:min(900px,92%); margin:18px auto 0; padding:18px 22px; border-left:5px solid var(--cyan); border-radius:8px; background:white; box-shadow:0 5px 18px rgba(3,67,117,.08); }} .ribbon-info.visible {{ display:block; }} .ribbon-info h2 {{ margin:0 0 6px; font-size:18px; }} .ribbon-info p {{ margin:0; color:#475569; }}
        .doctor-finder {{ display:grid; grid-template-columns:1.1fr 1.6fr; gap:20px; background:#f8fbfc; border:1px solid #dfeaf0; border-radius:10px; overflow:hidden; }} .doctor-panel {{ padding:18px; }} .doctor-panel.left {{ background:#eef9fb; border-right:1px solid #dfeaf0; }} .finder-label {{ margin:16px 0 10px; font-size:11px; font-weight:700; color:#475569; text-transform:uppercase; letter-spacing:.08em; }} .doctor-chips {{ display:flex; flex-wrap:wrap; gap:8px; }} .doctor-chip {{ border:1px solid #c5dfe4; background:white; color:#0f172a; border-radius:999px; padding:8px 12px; font-size:12px; font-weight:700; cursor:pointer; }} .doctor-chip.active {{ background:var(--blue); border-color:var(--blue); color:white; }} .doctor-toolbar {{ display:flex; align-items:center; justify-content:space-between; gap:12px; padding-bottom:12px; border-bottom:1px solid #dfeaf0; margin-bottom:12px; }} .reset-btn {{ border:0; background:var(--blue); color:white; font-weight:700; border-radius:6px; padding:8px 12px; cursor:pointer; }} .doctor-list {{ display:grid; gap:12px; }} .doctor-row {{ display:grid; grid-template-columns:1.3fr 1fr .8fr; gap:12px; align-items:center; padding:12px 12px; background:white; border:1px solid #e2e8f0; border-radius:8px; }} .doctor-row.empty {{ display:block; text-align:center; color:#475569; }} .doctor-profile {{ display:flex; flex-direction:column; gap:3px; }} .doctor-profile strong {{ color:var(--blue); font-size:14px; }} .doctor-profile span {{ font-size:12px; color:#475569; }} .doctor-meta {{ display:flex; flex-direction:column; gap:5px; }} .status-pill {{ display:inline-block; width:max-content; background:#ecfeff; color:#0b728a; font-size:11px; font-weight:700; padding:4px 8px; border-radius:999px; }} .doctor-meta small, .doctor-contact small {{ color:#64748b; font-size:11px; }} .doctor-contact {{ display:flex; flex-direction:column; gap:5px; align-items:flex-end; }} .doctor-contact strong {{ font-size:13px; color:#0f172a; }}
        body {{ font-family:Georgia, 'Times New Roman', serif; }} header, nav, .button, input, select, label, .action-tile, .stat-card, .feature-card, .doctor-finder, .ribbon-info {{ font-family:'Trebuchet MS', Arial, sans-serif; }}
        header {{ position:relative; overflow:hidden; }} header::after {{ content:''; position:absolute; left:7%; bottom:0; width:118px; height:3px; background:var(--cyan); }}
        .hero {{ position:relative; isolation:isolate; overflow:hidden; width:calc(100% + 8%); margin-left:-4%; min-height:470px; }} .hero::after {{ content:''; position:absolute; inset:0; z-index:-1; background:linear-gradient(90deg, rgba(3,39,51,.72), rgba(6,78,91,.2) 58%, rgba(4,40,52,.18)); }} .hero-copy {{ animation:hero-rise .7s ease-out both; }}
        .action-grid, .stats, .feature-strip, main > .card {{ animation:content-rise .6s ease-out both; }} .action-grid {{ animation-delay:.08s; }} .stats {{ animation-delay:.16s; }} .feature-strip {{ animation-delay:.24s; }}
        .action-tile {{ position:relative; overflow:hidden; border:1px solid rgba(255,255,255,.12); }} .action-tile::after {{ content:'→'; position:absolute; right:18px; bottom:14px; color:rgba(255,255,255,.72); font-size:20px; transition:transform .2s ease; }} .action-tile:hover::after, .action-tile:focus-visible::after {{ transform:translateX(4px); }}
        .action-grid {{ display:flex; justify-content:center; flex-wrap:wrap; gap:20px; margin:36px auto 40px; }} .action-grid .action-tile {{ flex:0 1 340px; min-height:104px; padding:22px 26px; border-radius:14px; box-shadow:0 12px 28px rgba(10,82,105,.22); }} .action-grid .action-tile strong {{ font-size:16px; }} .action-grid .action-tile small {{ font-size:12px; margin-top:8px; }}
        .stat-card {{ position:relative; overflow:hidden; }} .stat-card::before {{ content:''; position:absolute; left:0; top:0; bottom:0; width:4px; background:var(--cyan); }} .feature-card {{ transition:transform .2s ease, box-shadow .2s ease; }} .feature-card:hover {{ transform:translateY(-3px); box-shadow:0 14px 26px rgba(8,26,34,.1); }}
        .button {{ transition:transform .2s ease, box-shadow .2s ease, background .2s ease; box-shadow:0 7px 14px rgba(5,61,79,.16); }} .button:hover, .button:focus-visible {{ transform:translateY(-2px); box-shadow:0 10px 20px rgba(5,61,79,.24); }}
        @keyframes hero-rise {{ from {{ opacity:0; transform:translateY(16px); }} to {{ opacity:1; transform:translateY(0); }} }} @keyframes content-rise {{ from {{ opacity:0; transform:translateY(10px); }} to {{ opacity:1; transform:translateY(0); }} }} @media(prefers-reduced-motion:reduce) {{ *, *::before, *::after {{ animation-duration:.01ms !important; transition-duration:.01ms !important; }} }}
        @media(max-width:900px) {{ header {{ align-items:flex-start; flex-wrap:wrap; }} header > a:first-child {{ flex:1 1 440px; }} header .button {{ margin-top:0; }} nav {{ grid-template-columns:repeat(3, minmax(0, 1fr)); }} main {{ padding:0 3%; }} .card, .action-grid {{ width:100%; }} }}
        @media(max-width:650px) {{ header {{ flex-direction:column; gap:10px; padding:16px 5% 14px; background:linear-gradient(135deg, #ffffff 0%, #effbfd 100%); }} header > a:first-child {{ flex:0 0 auto; width:100%; font-size:20px; line-height:1.2; }} header::after {{ left:5%; width:92px; }} header .button {{ width:100%; text-align:center; margin-top:2px; }} nav {{ grid-template-columns:repeat(2, minmax(0, 1fr)); }} .nav-item > a {{ min-height:52px; padding:10px 6px; font-size:10px; }} .dropdown {{ position:fixed; left:4%; right:4%; min-width:0; }} main {{ width:100%; margin:20px auto; padding:0 4%; }} .hero {{ width:calc(100% + 8%); margin-left:-4%; min-height:360px; }} .card {{ padding:18px; }} .grid, .action-grid, .doctor-finder, .facility-layout {{ grid-template-columns:1fr; }} .facility-photo {{ min-height:240px; max-height:300px; }} .facility-copy {{ padding:20px; }} .doctor-row {{ grid-template-columns:1fr; }} .doctor-contact {{ align-items:flex-start; }} .button {{ width:100%; text-align:center; }} }}
        @media(max-width:380px) {{ header > a:first-child {{ font-size:17px; }} nav {{ grid-template-columns:1fr; }} .hero {{ min-height:240px; padding:28px 14px; }} .hero-copy {{ padding:12px 8px; }} h1 {{ font-size:24px; }} }}
        .site-search {{ display:flex; align-items:center; gap:6px; }} .site-search input {{ width:190px; height:46px; padding:10px 14px; border-radius:999px; background:white; }} .site-search button {{ min-height:46px; margin:0; padding:10px 16px; border-radius:999px; background:var(--teal); }} header {{ display:grid; grid-template-columns:minmax(0,1fr) auto auto; align-items:center; }} header .site-search {{ justify-self:end; margin-right:8px; }} header > .button.green {{ min-height:46px; margin-top:0; display:flex; align-items:center; justify-content:center; }}
        @media(max-width:900px) {{ header {{ display:flex; }} header .site-search {{ margin-right:0; }} }}
        @media(max-width:650px) {{ .site-search {{ width:100%; }} .site-search input {{ flex:1; width:auto; }} }}
        {ENTRY_PORTAL_CSS}
        {PORTAL_PROFILE_CSS}
        </style></head><body class="{escape(page_class, quote=True)}"><header><a href="/">GAVINO Y. ALVAREZ PRIMARY CARE CENTER&nbsp;<span>Diagnostics &amp; Urgent Care</span></a><form class="site-search" action="/search" method="get"><input name="q" aria-label="Search GAPCC" placeholder="Search services or doctors"><button class="button" type="submit">Search</button></form></header>
        <nav><div class="nav-item"><a href="/" onclick="window.location.href='/'; return false;">Home</a></div><div class="nav-item"><a href="#" onclick="showRibbonInfo('FIND US','Find GAPCC facilities and contact information.'); return false;">FIND US</a><div class="dropdown"><a href="#" onclick="showRibbonInfo('Facility location','Gavino Alvarez Lying-in Center, R. Magsaysay Ave, Brgy. Narra, San Pedro City, Laguna, Philippines.'); return false;">Facility location</a><a href="#" onclick="showRibbonInfo('Operating Hours','Contact the selected facility to confirm current operating hours.'); return false;">Operating Hours</a><a href="#" onclick="showRibbonInfo('Contact Information','Contact the GAPCC facility desk for assistance.'); return false;">Contact Information</a></div></div><div class="nav-item"><a href="#" onclick="showDoctorFinder(); return false;">Care Services</a><div class="dropdown"><a href="#" onclick="showDoctorFinder(); return false;">Find a Doctor</a><a href="#" onclick="showRibbonInfo('Vaccination','Review vaccination information.'); return false;">Vaccination</a><a href="#" onclick="showRibbonInfo('Blood pressure check','Request a screening.'); return false;">Blood pressure check</a><a href="#" onclick="showRibbonInfo('Ambulance transport service','Contact urgent care staff.'); return false;">Ambulance transport service</a><a href="#" onclick="showRibbonInfo('Free medical consultations','Ask about current availability.'); return false;">Free medical consultations</a></div></div><div class="nav-item"><a href="#" onclick="showRibbonInfo('Results','Choose your results pathway.'); return false;">Results</a><div class="dropdown"><a href="#" onclick="showRibbonInfo('Patient','Use the Existing Patient Portal.'); return false;">Patient</a><a href="#" onclick="showRibbonInfo('Doctor','Review doctor-facing result access.'); return false;">Doctor</a></div></div><div class="nav-item"><a href="#" onclick="showRibbonInfo('Test Directory','Browse laboratory and diagnostic procedures.'); return false;">Test Directory</a><div class="dropdown"><a href="#" onclick="showRibbonInfo('Test packages','Review grouped service packages.'); return false;">Test packages</a><a href="#" onclick="showRibbonInfo('Featured test','See highlighted tests.'); return false;">Featured test</a><a href="#" onclick="showRibbonInfo('Test guidelines','Review preparation guidance.'); return false;">Test guidelines</a></div></div><div class="nav-item"><a href="#" onclick="showRibbonInfo('Resources','Open GAPCC health information.'); return false;">Resources</a><div class="dropdown"><a href="#" onclick="showRibbonInfo('Promos and Packages','Review current promotions.'); return false;">Promos and Packages</a><a href="#" onclick="showRibbonInfo('News and event','View GAPCC updates.'); return false;">News and event</a><a href="#" onclick="showRibbonInfo('Health library','Browse health education.'); return false;">Health library</a></div></div></nav><section class="ribbon-info" id="ribbon-info" aria-live="polite"><h2 id="ribbon-info-title"></h2><p id="ribbon-info-text"></p></section><main>{body}</main><script>function showRibbonInfo(title, text) {{ const panel=document.getElementById('ribbon-info'); panel.querySelector('h2').textContent=title; panel.querySelector('p').textContent=text; panel.classList.add('visible'); panel.scrollIntoView({{behavior:'smooth', block:'nearest'}}); }}</script></body></html>"""
    )


def registration_form(message=""):
    notice = f'<div class="notice">{escape(message)}</div>' if message else ""
    return page(
        "Patient Pre-Registration",
        f"""<section class="card"><h1>Step 1: Patient Pre-Registration</h1>
        <p>Enter your demographic details once. GAPCC will create your permanent patient portal account before scheduling.</p>{notice}
        <form action="/register" method="post"><div class="grid">
        <div><label for="last_name">Surname</label><input id="last_name" name="last_name" required></div>
        <div><label for="given_name">Given Name</label><input id="given_name" name="given_name" required></div>
        <div><label for="middle_name">Middle Name</label><input id="middle_name" name="middle_name"></div>
        <div><label for="gender">Gender</label><select id="gender" name="gender" required>{option_markup(GENDER_OPTIONS)}</select></div>
        <div><label for="date_of_birth">Birthday</label><input id="date_of_birth" type="date" name="date_of_birth" max="{datetime.now(timezone.utc).date().isoformat()}" required></div>
        <div><label for="email">Email Address</label><input id="email" type="email" name="email" required></div>
        <div><label for="contact_number">Philippine Contact Number</label><div class="phone-field"><span>+63</span><input id="contact_number" type="tel" name="contact_number" inputmode="numeric" placeholder="9171234567 or 09171234567" maxlength="11" required></div></div>
        </div><button class="button" type="submit">Create Portal Account and Continue</button></form><p class="entry-signup">Already have an account? <a href="/portal">Log In</a></p><a class="entry-home" href="/">Back to GAPCC Home</a></section>""",
        page_class="entry-shell entry-patient entry-booking",
    )


HOME_LOGIN_CSS = """<style>
.hero { justify-content:space-between !important; gap:40px; text-align:left !important; padding:44px 6% !important; }
.hero .hero-copy { flex:1 1 0; max-width:560px; margin:0; }
.hero .glass-card { flex:0 0 380px; max-width:100%; box-sizing:border-box; padding:26px 28px; border:1px solid rgba(255,255,255,.55); border-radius:18px; background:rgba(22,34,31,.42); color:#fff; box-shadow:0 24px 70px rgba(0,0,0,.24); backdrop-filter:blur(16px); -webkit-backdrop-filter:blur(16px); font-family:'Trebuchet MS',Arial,sans-serif; }
.hero .glass-card h2 { margin:0 0 6px; color:#fff; font-size:24px; }
.hero .glass-card p { color:rgba(255,255,255,.88); margin:0 0 12px; font-size:13px; }
.hero .glass-card label { display:block; margin:10px 0 4px; color:rgba(255,255,255,.9); font-size:11px; text-transform:uppercase; letter-spacing:.04em; }
.hero .glass-card input { width:100%; box-sizing:border-box; min-height:44px; padding:0 12px; border:1px solid rgba(255,255,255,.48); border-radius:11px; background:rgba(255,255,255,.12); color:#fff; }
.hero .glass-card input::placeholder { color:rgba(255,255,255,.68); }
.hero .phone-row { display:flex; gap:6px; }
.hero .phone-row span { display:flex; align-items:center; padding:0 12px; border-radius:11px; background:rgba(255,255,255,.9); color:#17353a; font-weight:700; font-size:13px; }
.hero .glass-button { display:block; width:100%; margin-top:16px; padding:13px; border:0; border-radius:11px; background:linear-gradient(90deg,#c3d81f,#27b36a); color:#fff; font-weight:800; cursor:pointer; }
.hero .glass-links { display:flex; justify-content:center; flex-wrap:wrap; gap:8px 20px; margin-top:14px; text-align:center; font-size:12px; color:rgba(255,255,255,.9); }
.hero .glass-links a { color:#bfeaf0; font-weight:700; text-decoration:none; }
.hero .glass-links a:hover { text-decoration:underline; }
@media(max-width:900px) { .hero { flex-direction:column; justify-content:center !important; padding:28px 4% !important; } .hero .glass-card { flex:0 0 auto; width:100%; } }
</style>"""

HOME_LOGIN_HTML = HOME_LOGIN_CSS + (
    '<div class="glass-card" id="patient-login"><h2>Patient Portal Log In</h2><p>Enter your patient ID, registered email, and contact number to view your portal.</p>'
    '<form action="/portal" method="post"><label for="home_patient_id">Patient ID</label><input id="home_patient_id" name="patient_id" placeholder="GAPCC-P-XXXXXXXXXXXX" required>'
    '<label for="home_email">Registered Email</label><input id="home_email" type="email" name="email" required>'
    '<label for="home_contact">Philippine Contact Number</label><div class="phone-row"><span>+63</span><input id="home_contact" type="tel" name="contact_number" inputmode="numeric" placeholder="9171234567 or 09171234567" maxlength="11" required></div>'
    '<button class="glass-button" type="submit">Log In to Patient Portal</button></form>'
    '<div class="glass-links"><a href="/portal/recover">Forgot your patient ID?</a><a href="/register">Create Account</a></div></div>'
)


@app.get("/", response_class=HTMLResponse)
def home():
    home_body = """<script>document.querySelector('nav .nav-item a[href="/"]').onclick = function() { window.location.href = '/'; }; document.addEventListener('DOMContentLoaded', function() { const facilitySection = document.getElementById('facility-location'); const contactSection = document.getElementById('contact-information'); const hoursSection = document.getElementById('operating-hours'); const labSection = document.getElementById('laboratory-packages'); const ribbon = document.getElementById('ribbon-info'); [facilitySection, contactSection, hoursSection, labSection].forEach(function(section) { if (section) section.style.display = 'none'; }); [facilitySection, contactSection, hoursSection, labSection].forEach(function(section) { if (section && ribbon) ribbon.after(section); }); const reveal = function(section, event) { event.preventDefault(); [facilitySection, contactSection, hoursSection, labSection].forEach(function(item) { if (item) item.style.display = item === section ? 'block' : 'none'; }); section.scrollIntoView({behavior:'smooth'}); }; document.querySelectorAll('nav a, .action-tile').forEach(function(link) { const label = link.textContent.trim().toLowerCase(); if (label.includes('facility location') || label === 'find us') { link.href = '#facility-location'; link.onclick = reveal.bind(null, facilitySection); } if (label.includes('contact information')) { link.href = '#contact-information'; link.onclick = reveal.bind(null, contactSection); } if (label.includes('operating hours')) { link.href = '#operating-hours'; link.onclick = reveal.bind(null, hoursSection); } if (label.includes('test directory')) { link.href = '#laboratory-packages'; link.onclick = reveal.bind(null, labSection); } }); });</script><section class="hero"><div class="hero-copy"><h1>Care Beyond Cure</h1><p>Caring for you and the ones you love most. Welcome to GAPCC.</p></div><!--HERO-LOGIN--></section>
        <section class="action-grid" aria-label="GAPCC portal access"><a class="action-tile" href="/doctor"><strong>Doctor Portal Log In</strong><small>Open the clinical dashboard and doctor profile</small></a><a class="action-tile" href="/admin"><strong>Admin Portal Log In</strong><small>Open the administration dashboard and profile</small></a></section>
        <section class="stats"><div class="stat-card"><span class="label">Patients Served</span><span class="value">12.4K</span><span class="note">Across outpatient, urgent care, and diagnostics</span></div><div class="stat-card"><span class="label">Avg. Response</span><span class="value">14 min</span><span class="note">From check-in to physician consultation</span></div><div class="stat-card"><span class="label">Facilities</span><span class="value">3</span><span class="note">Main, satellite, and specialist clinics</span></div><div class="stat-card"><span class="label">Coverage</span><span class="value">24/7</span><span class="note">Urgent care and essential emergency support</span></div></section>
        <section class="facility-section" id="facility-location"><div class="facility-layout"><img class="facility-photo" src="/assets/800436033_1604320875067949_4721912843219203405_n%20(1).jpg" alt="Exterior of Gavino Y. Alvarez Primary Care Center"><div class="facility-copy"><p class="eyebrow">Find Us</p><h2>Gavino Y. Alvarez Primary Care Center</h2><p>Visit our main facility for primary care, urgent consultations, diagnostics, and patient support.</p><div class="facility-address"><span aria-hidden="true">●</span><div><strong>Facility Address</strong>Gavino Alvarez Lying-in Center<br>R. Magsaysay Ave, Brgy. Narra<br>San Pedro City, Laguna, Philippines</div></div><div class="facility-actions"><a class="map-link" href="https://www.google.com/maps/search/?api=1&amp;query=Gavino+Alvarez+Lying-in+Center%2C+R.+Magsaysay+Ave%2C+Brgy.+Narra%2C+San+Pedro+City%2C+Laguna%2C+Philippines" target="_blank" rel="noopener">Open in Google Maps</a><a class="map-link secondary" href="https://www.google.com/maps/dir/?api=1&amp;destination=Gavino+Alvarez+Lying-in+Center%2C+R.+Magsaysay+Ave%2C+Brgy.+Narra%2C+San+Pedro+City%2C+Laguna%2C+Philippines" target="_blank" rel="noopener">Get Directions</a></div><iframe class="facility-map" title="GAPCC location map" src="https://www.google.com/maps?q=Gavino+Alvarez+Lying-in+Center%2C+R.+Magsaysay+Ave%2C+Brgy.+Narra%2C+San+Pedro+City%2C+Laguna%2C+Philippines&amp;output=embed" loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe></div></div></section>
        <section class="facility-section" id="contact-information"><style>#contact-information .facility-contact a { font-family:'Trebuchet MS',Arial,sans-serif; font-size:16px; letter-spacing:.02em; } #contact-information .contact-layout { display:grid; grid-template-columns:180px 1fr; gap:22px; align-items:center; } #contact-information .contact-photo { width:180px; height:150px; object-fit:cover; border-radius:10px; } @media(max-width:650px) { #contact-information .contact-layout { grid-template-columns:1fr; } #contact-information .contact-photo { width:100%; height:190px; } }</style><div class="facility-copy" style="background:#fff;border:1px solid #dfeaf0;border-radius:14px;box-shadow:0 10px 28px var(--shadow);"><div class="contact-layout"><img class="contact-photo" src="/assets/800436033_1604320875067949_4721912843219203405_n%20(1).jpg" alt="Gavino Y. Alvarez Primary Care Center"><div><p class="eyebrow">Contact Information</p><h2>GAPCC Contact Numbers</h2><p>Call the appropriate desk for assistance with clinic services and urgent care.</p><div class="facility-contact"><a href="tel:+63284786270"><span>Admin</span>(02) 8-478-6270</a><a href="tel:+63285190249"><span>ER / Urgent Care</span>(02) 8-519-0249</a></div></div></div></div></section>
        <section class="facility-section" id="operating-hours"><style>#operating-hours .hours-intro { margin-bottom:18px; } #operating-hours .hours-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; } #operating-hours .hours-card { overflow:hidden; background:#fff; border:1px solid #dfeaf0; border-radius:10px; box-shadow:0 8px 18px rgba(8,26,34,.05); } #operating-hours .hours-card img { width:100%; height:135px; object-fit:cover; display:block; } #operating-hours .hours-card-body { padding:14px 16px 16px; } #operating-hours .hours-card h3 { margin:0 0 8px; color:var(--blue); font-size:15px; } #operating-hours .hours-card p { margin:0; color:#0e7d87; font-family:'Trebuchet MS',Arial,sans-serif; font-size:17px; font-weight:800; } @media(max-width:650px) { #operating-hours .hours-grid { grid-template-columns:1fr; } }</style><div class="facility-copy" style="background:#fff;border:1px solid #dfeaf0;border-radius:14px;box-shadow:0 10px 28px var(--shadow);"><p class="eyebrow">Operating Hours</p><h2>Clinic Service Hours</h2><p class="hours-intro">Service availability by department. Emergency and urgent care support remains open around the clock.</p><div class="hours-grid"><article class="hours-card"><img src="https://images.unsplash.com/photo-1606811971618-4486d14f3f99?auto=format&amp;fit=crop&amp;w=900&amp;q=82" alt="Dental care equipment"><div class="hours-card-body"><h3>Dental Services</h3><p>8:00 AM - 3:00 PM</p></div></article><article class="hours-card"><img src="https://images.unsplash.com/photo-1551076805-e1869033e561?auto=format&amp;fit=crop&amp;w=900&amp;q=82" alt="Maternal care consultation room"><div class="hours-card-body"><h3>Prenatal / Postnatal Care</h3><p>8:00 AM - 3:00 PM</p></div></article><article class="hours-card"><img src="https://images.unsplash.com/photo-1579154204601-01588f351e67?auto=format&amp;fit=crop&amp;w=900&amp;q=82" alt="Laboratory testing equipment"><div class="hours-card-body"><h3>Laboratory</h3><p>8:00 AM - 3:00 PM</p></div></article><article class="hours-card"><img src="https://images.unsplash.com/photo-1516841273335-e39b37888115?auto=format&amp;fit=crop&amp;w=900&amp;q=82" alt="Outpatient medical consultation"><div class="hours-card-body"><h3>OPD</h3><p>8:00 AM - 4:00 PM</p></div></article><article class="hours-card"><img src="https://images.unsplash.com/photo-1587351021759-3e566b6af7cc?auto=format&amp;fit=crop&amp;w=900&amp;q=82" alt="Emergency care team in hospital"><div class="hours-card-body"><h3>Emergency / Urgent Care</h3><p>24 hours / 7 days</p></div></article></div></div></section>
        <section class="facility-section" id="laboratory-packages"><style>#laboratory-packages .lab-layout { display:grid; grid-template-columns:180px 1fr; gap:22px; align-items:start; } #laboratory-packages .lab-photo { width:180px; height:230px; object-fit:cover; border-radius:10px; } #laboratory-packages .lab-groups { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; } #laboratory-packages .lab-group { padding:14px; background:#f7fbfc; border:1px solid #dfeaf0; border-radius:9px; } #laboratory-packages .lab-group h3 { margin:0 0 8px; color:var(--blue); font-size:14px; } #laboratory-packages .lab-group p { margin:0; color:#526b72; font-size:12px; line-height:1.65; } @media(max-width:650px) { #laboratory-packages .lab-layout, #laboratory-packages .lab-groups { grid-template-columns:1fr; } #laboratory-packages .lab-photo { width:100%; height:220px; } }</style><div class="facility-copy" style="background:#fff;border:1px solid #dfeaf0;border-radius:14px;box-shadow:0 10px 28px var(--shadow);"><div class="lab-layout"><img class="lab-photo" src="/assets/813824845_1378616194436907_2471595001531605655_n.jpg" alt="Laboratory blood sample testing"><div><p class="eyebrow">Test Directory</p><h2>Laboratory Packages</h2><p>Choose from available laboratory examinations for your appointment request.</p><div class="lab-groups"><div class="lab-group"><h3>Clinical Microscopy</h3><p>Urinalysis, Fecalysis, Pregnancy Test, Occult Blood, Urine Ketone, Semanalysis</p></div><div class="lab-group"><h3>Hematology</h3><p>CBC, Platelet Count, Hgb/Hct, Differential Count, Clotting Time, Bleeding Time, ESR, Toxic Granules</p></div><div class="lab-group"><h3>Clinical Chemistry</h3><p>FBS, BUN, CREA, BUA, Cholesterol, Triglycerides, HDL, LDL, SGPT/ALT, SGOT/AST, OGTT, Sodium, Potassium, Chloride, HGT</p></div><div class="lab-group"><h3>Serology / Immunology</h3><p>HBsAg, Anti-HCV, Anti-HAV IgM, HIV Test, Syphilis ICT/RPR, Dengue, Typhoid, HbA1c, Troponin, CK-MB, PSA, TSH</p></div><div class="lab-group"><h3>Blood Station</h3><p>Blood Typing, RH Typing, Cross Matching, Newborn Screening, Other Tests</p></div></div></div></div></div></section>
        <script>document.addEventListener('DOMContentLoaded', function() { const hoursGrid = document.querySelector('#operating-hours .hours-grid'); if (!hoursGrid) return; [['X-ray','X-ray imaging equipment','https://images.unsplash.com/photo-1511174511562-5f7f18b874f8?auto=format&fit=crop&w=900&q=82'],['Drug Test','Drug test laboratory equipment','https://images.unsplash.com/photo-1582719478250-c89cae4dc85b?auto=format&fit=crop&w=900&q=82'],['Imaging','Medical imaging consultation','https://images.unsplash.com/photo-1579684385127-1ef15d508118?auto=format&fit=crop&w=900&q=82']].forEach(function(service) { const card = document.createElement('article'); card.className = 'hours-card'; card.innerHTML = '<img src="' + service[2] + '" alt="' + service[1] + '"><div class="hours-card-body"><h3>' + service[0] + '</h3><p>8:00 AM - 3:00 PM</p></div>'; hoursGrid.appendChild(card); }); });</script><section class="feature-strip"><div class="feature-card"><div class="feature-icon">✓</div><div><h3>Pre-Registration</h3><p>Complete your patient intake once and keep your record ready.</p></div></div><div class="feature-card"><div class="feature-icon">◎</div><div><h3>Fast Access</h3><p>Check in, confirm service, and view your next steps faster.</p></div></div><div class="feature-card"><div class="feature-icon">+</div><div><h3>Unified Care</h3><p>Clinical, imaging, and scheduled care all in one portal.</p></div></div></section>
"""
    return page("GAPCC Portal", home_body.replace("<!--HERO-LOGIN-->", HOME_LOGIN_HTML))


@app.get("/register", response_class=HTMLResponse)
def register_page():
    return registration_form()


STAFF_EMAIL_DOMAIN = "@gapcc.local"
CONTACT_EMAIL_PATTERN = r"[^@\s]+@[^@\s]+\.[^@\s]+"


def notification_address(account):
    contact = (account["contact_email"] or "").strip()
    if contact:
        return contact
    login_email = account["email"]
    return "" if login_email.endswith(STAFF_EMAIL_DOMAIN) else login_email


def account_request_form(account_type, message="", email="", contact_email=""):
    if account_type not in {"admin", "doctor"}:
        return HTMLResponse("Account type not found.", status_code=404)
    title = "Administrator Account Request" if account_type == "admin" else "Doctor Account Request"
    notice = f'<div class="notice error">{escape(message)}</div>' if message else ""
    doctor_fields = ""
    if account_type == "doctor":
        doctor_fields = f"""<div><label for="specialization">Specialization</label><select id="specialization" name="specialization" required><option value="">Choose a specialization</option>{option_markup(DOCTOR_SPECIALIZATIONS)}</select></div><div><label for="facility">Facility</label><select id="facility" name="facility" required>{option_markup(FACILITIES)}</select></div><div><label for="registration_number">License Number</label><input id="registration_number" name="registration_number" required></div>"""
    return page(
        title,
        f"""<section class="card"><h1>{escape(title)}</h1><p>Submit your details for administrator review. Access is enabled only after approval.</p>{notice}<form action="/account-request/{account_type}" method="post"><div class="grid"><div><label for="display_name">Full Name</label><input id="display_name" name="display_name" autocomplete="name" required></div><div><label for="username">Desired Username</label><div class="phone-field"><input id="username" name="username" value="{escape(email.removesuffix(STAFF_EMAIL_DOMAIN), quote=True)}" pattern="[A-Za-z0-9._\\-]{{3,32}}" minlength="3" maxlength="32" autocomplete="username" required><span>{STAFF_EMAIL_DOMAIN}</span></div><small>Your login email will be username{STAFF_EMAIL_DOMAIN}. Use 3-32 letters, numbers, dots, dashes, or underscores.</small></div>{doctor_fields}<div><label for="contact_email">Your Email / Gmail (required)</label><input id="contact_email" type="email" name="contact_email" value="{escape(contact_email, quote=True)}" autocomplete="email" placeholder="you@gmail.com" required><small>Approval results, account alerts, updates and password recovery codes are sent to this address.</small></div><div><label for="password">Password</label><input id="password" type="password" name="password" minlength="12" pattern="{PASSWORD_POLICY_PATTERN}" autocomplete="new-password" required><small>{PASSWORD_POLICY_MESSAGE}</small></div></div><button class="button" type="submit">Submit Account Request</button></form><a class="entry-home" href="/{account_type}">Back to Log In</a></section>""",
        page_class=f"entry-shell entry-{account_type}",
    )


@app.get("/account-request/{account_type}", response_class=HTMLResponse)
def account_request_page(account_type: str):
    return account_request_form(account_type)


@app.post("/account-request/{account_type}", response_class=HTMLResponse)
def submit_account_request(
    account_type: str,
    display_name: str = Form(...),
    username: str = Form(...),
    contact_email: str = Form(""),
    password: str = Form(...),
    specialization: str = Form(""),
    facility: str = Form(""),
    registration_number: str = Form(""),
):
    if account_type not in {"admin", "doctor"}:
        return HTMLResponse("Account type not found.", status_code=404)
    clean_name = display_name.strip()
    clean_username = username.strip().lower().removesuffix(STAFF_EMAIL_DOMAIN)
    clean_email = f"{clean_username}{STAFF_EMAIL_DOMAIN}"
    clean_contact = contact_email.strip().lower()
    if not re.fullmatch(CONTACT_EMAIL_PATTERN, clean_contact) or clean_contact.endswith(STAFF_EMAIL_DOMAIN):
        return account_request_form(account_type, "Enter a real email or Gmail address you can access for notifications.", clean_email, clean_contact)
    if not re.fullmatch(r"[a-z0-9._-]{3,32}", clean_username):
        return account_request_form(account_type, "Choose a username of 3-32 letters, numbers, dots, dashes, or underscores.", clean_email)
    if not clean_name or not password_meets_policy(password):
        return account_request_form(account_type, f"Enter your name and a password that meets this policy: {PASSWORD_POLICY_MESSAGE}", clean_email)
    clean_specialization = specialization.strip()
    clean_facility = facility.strip()
    clean_registration = registration_number.strip()
    if account_type == "doctor" and (
        clean_specialization not in DOCTOR_SPECIALIZATIONS
        or clean_facility not in FACILITIES
        or not clean_registration
    ):
        return account_request_form(account_type, "Complete all doctor details using a listed facility.", clean_email)
    with connect_db() as connection:
        existing_account = connection.execute(
            """SELECT 1 FROM admins WHERE email = ?
            UNION ALL SELECT 1 FROM doctors WHERE email = ?
            UNION ALL SELECT 1 FROM account_requests WHERE email = ? AND status = 'Pending'
            LIMIT 1""",
            (clean_email, clean_email, clean_email),
        ).fetchone()
        if existing_account:
            return account_request_form(account_type, "An account or pending request already exists for this email.", clean_email)
        connection.execute(
            """INSERT INTO account_requests
            (account_type, display_name, email, contact_email, password_hash, specialization, facility, registration_number, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                account_type,
                clean_name,
                clean_email,
                clean_contact,
                token_hash(password),
                clean_specialization,
                clean_facility,
                clean_registration,
                utc_now(),
            ),
        )
        admin_emails = [
            address for address in (notification_address(row) for row in connection.execute("SELECT email, contact_email FROM admins").fetchall()) if address
        ]
    admin_notification_statuses = [
        send_email_message(
            admin_email,
            f"GAPCC {account_type.title()} Account Request",
            f"A new {account_type} account request is awaiting review.\n\nName: {clean_name}\nUsername: {clean_email}\nContact email: {clean_contact}\nReview it in the Admin Console account requests section.",
        )
        for admin_email in admin_emails
    ]
    account_notification_status = send_email_message(
        clean_contact,
        f"GAPCC {account_type.title()} Account Request Received",
        f"Hello {clean_name},\n\nYour GAPCC {account_type} account request for {clean_email} was received and is pending administrator approval. Approval results, account updates and password recovery codes will be sent to this email address.",
    )
    if account_notification_status == "Sent":
        notification_message = "A confirmation and future account alerts will be sent to your email address."
    elif account_notification_status == "Not Configured":
        notification_message = "Email delivery is not configured; your request is saved for administrator review, but email alerts and recovery codes cannot be delivered yet."
    else:
        notification_message = "The request is saved for administrator review, but email delivery failed. Verify your address with the clinic administrator."
    if "Sent" in admin_notification_statuses:
        notification_message += " Administrators were also notified."
    elif admin_notification_statuses and all(status == "Not Configured" for status in admin_notification_statuses):
        notification_message += " The Admin Console still contains your request."
    return page(
        "Account Request Submitted",
        f"""<section class="card"><h1>Request submitted</h1><p>Your {escape(account_type)} account request is pending administrator approval. You can sign in after it is approved.</p><p>Your login username is <strong>{escape(clean_email)}</strong>. Notifications go to <strong>{escape(clean_contact)}</strong>.</p><p>{escape(notification_message)}</p><p>Once an administrator decides, check the result on the <a href="/account-request/{account_type}/status">request status page</a>.</p><a class="button" href="/{account_type}">Return to Log In</a></section>""",
        page_class=f"entry-shell entry-{account_type}",
    )


def portal_form(message=""):
    notice = f'<div class="notice error">{escape(message)}</div>' if message else ""
    return page(
        "Existing Patient Portal",
        f"""<section class="card"><h1>Existing Patient Portal</h1>
        <p>Enter your patient ID, registered email, and contact number to view your portal.</p>{notice}
        <form action="/portal" method="post"><div class="grid">
        <div><label for="patient_id">Patient ID</label><input id="patient_id" name="patient_id" placeholder="GAPCC-P-XXXXXXXXXXXX" required></div>
        <div><label for="email">Registered Email</label><input id="email" type="email" name="email" required></div>
        <div><label for="contact_number">Philippine Contact Number</label><div class="phone-field"><span>+63</span><input id="contact_number" type="tel" name="contact_number" inputmode="numeric" placeholder="9171234567 or 09171234567" maxlength="11" required></div></div>
        </div><button class="button" type="submit">Log In to Patient Portal</button></form>
        </section><div class="portal-login-links"><a href="/portal/recover">Forgot your patient ID</a><a href="/register">Create Account</a></div>""",
        page_class="entry-shell entry-patient",
    )


@app.get("/portal", response_class=HTMLResponse)
def portal_page(request: Request):
    token = request.cookies.get(PORTAL_SESSION_COOKIE, "")
    claim = verify_access_token(token)
    if claim and claim[0] == "patient":
        with connect_db() as connection:
            patient = connection.execute(
                "SELECT patient_id, email, contact_number FROM patients WHERE patient_id = ? AND portal_activated = 1",
                (claim[1],),
            ).fetchone()
        if patient:
            return open_portal(patient["patient_id"], patient["email"], patient["contact_number"])
    return portal_form()


@app.get("/portal/logout")
def patient_portal_logout():
    response = RedirectResponse(url="/portal", status_code=303)
    return clear_portal_session(response)


@app.get("/portal/recover", response_class=HTMLResponse)
def recover_patient_id_page():
    return page(
        "Recover Patient ID",
        """<section class="card"><h1>Recover Patient ID</h1>
        <p>Verify the email and contact number used during registration to retrieve your patient ID.</p>
        <form action="/portal/recover" method="post"><div class="grid">
        <div><label for="email">Registered Email</label><input id="email" type="email" name="email" required></div>
        <div><label for="contact_number">Philippine Contact Number</label><div class="phone-field"><span>+63</span><input id="contact_number" type="tel" name="contact_number" inputmode="numeric" placeholder="9171234567 or 09171234567" maxlength="11" required></div></div>
        </div><button class="button" type="submit">Retrieve Patient ID</button></form></section>""",
    )


@app.get("/results/patient", response_class=HTMLResponse)
def patient_results_login_page():
    return page(
        "Patient Results Login",
        """<section class="card"><h1>Patient Laboratory Results</h1><p>Enter your registered patient account number and choose where to receive your one-time password.</p>
        <form action="/results/patient/request" method="post"><div class="grid"><div><label for="patient_id">Patient Account Number</label><input id="patient_id" name="patient_id" placeholder="GAPCC-P-XXXXXXXXXXXX" required></div><div><label for="channel">Send OTP To</label><select id="channel" name="channel"><option value="email">Registered email</option><option value="phone">Registered phone</option></select></div></div><button class="button green" type="submit">Send OTP</button></form></section>""",
    )


@app.post("/results/patient/request", response_class=HTMLResponse)
def request_patient_results_otp(patient_id: str = Form(...), channel: str = Form(...)):
    if channel not in {"email", "phone"}:
        return page("Results Login Error", '<section class="card error"><h1>Invalid OTP channel</h1><p>Choose email or phone.</p><a class="button" href="/results/patient">Try Again</a></section>')
    with connect_db() as connection:
        patient = connection.execute("SELECT patient_id, email, contact_number, portal_activated FROM patients WHERE patient_id = ?", (patient_id.strip(),)).fetchone()
        if patient is None or not patient["portal_activated"]:
            return page("Results Login Error", '<section class="card error"><h1>Patient account not found</h1><p>Use an active registered patient account number.</p><a class="button" href="/results/patient">Try Again</a></section>')
        destination = patient["email"] if channel == "email" else patient["contact_number"]
        otp_id = secrets.token_urlsafe(18)
        otp_code = f"{secrets.randbelow(1_000_000):06d}"
        connection.execute("INSERT INTO result_access_otps (otp_id, patient_id, delivery_channel, destination, code_hash, expires_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)", (otp_id, patient["patient_id"], channel, destination, token_hash(otp_code), datetime.now(timezone.utc).timestamp() + 600, utc_now()))
    return page("Verify Results Access", f"""<section class="card"><h1>Verify Results Access</h1><p>We sent a one-time password to your registered {escape(channel)}.</p><p class="muted">Destination: {escape(destination)}</p><form action="/results/patient/verify" method="post"><input type="hidden" name="otp_id" value="{escape(otp_id, quote=True)}"><label for="otp_code">One-Time Password</label><input id="otp_code" name="otp_code" inputmode="numeric" maxlength="6" required><button class="button green" type="submit">Verify and View Results</button></form></section>""",
    )


@app.post("/results/patient/verify", response_class=HTMLResponse)
def verify_patient_results_otp(otp_id: str = Form(...), otp_code: str = Form(...)):
    with connect_db() as connection:
        otp = connection.execute("SELECT * FROM result_access_otps WHERE otp_id = ? AND used = 0", (otp_id,)).fetchone()
        if otp is None or float(otp["expires_at"]) < datetime.now(timezone.utc).timestamp() or otp["attempts"] >= 5:
            return page("Results OTP Error", '<section class="card error"><h1>OTP expired or unavailable</h1><p>Start the results login again.</p><a class="button" href="/results/patient">Try Again</a></section>')
        if token_hash(otp_code.strip()) != otp["code_hash"]:
            connection.execute("UPDATE result_access_otps SET attempts = attempts + 1 WHERE otp_id = ?", (otp_id,))
            return page("Results OTP Error", '<section class="card error"><h1>Incorrect OTP</h1><p>Check the code and try again.</p><a class="button" href="/results/patient">Request a new OTP</a></section>')
        connection.execute("UPDATE result_access_otps SET used = 1 WHERE otp_id = ?", (otp_id,))
        results = connection.execute("SELECT result_id, test_name, result_summary, collected_at, pdf_filename, pdf_data IS NOT NULL AS has_document FROM lab_results WHERE patient_id = ? AND result_status = 'Released' ORDER BY collected_at DESC, result_id DESC", (otp["patient_id"],)).fetchall()
        patient = connection.execute("SELECT * FROM patients WHERE patient_id = ?", (otp["patient_id"],)).fetchone()
    items = []
    patient_token = sign_access_token("patient", otp["patient_id"])
    for result in results:
        document = (
            result_document_preview(result["result_id"], patient_token, result["test_name"])
            if result["has_document"]
            else '<p class="muted">No uploaded document is available for this result.</p>'
        )
        items.append(
            f'<article class="patient-lab-result"><h3>{escape(result["test_name"])}</h3>'
            f'<p>{escape(result["result_summary"])}</p><p class="muted">Conducted: {escape(result["collected_at"])}</p>'
            f'{document}</article>'
        )
    result_list = "".join(items) or '<p class="muted">No released laboratory results are available yet.</p>'
    return page(
        "Patient Results",
        f"""<style>{RESULT_ACTION_CSS}.patient-lab-results{{display:grid;gap:22px}}.patient-lab-result{{padding:0 0 22px;border-bottom:1px solid #d9e8eb}}.patient-lab-result h3{{margin:12px 0 6px}}.patient-result-preview{{display:block;width:100%;height:760px;border:1px solid #cbd9dc;border-radius:8px;background:#eef4f5}}@media(max-width:700px){{.patient-result-preview{{height:560px}}}}</style>
        <section class="card"><h1>Results for {escape(patient_name(patient))}</h1><p>Verified patient account: <strong>{escape(patient['patient_id'])}</strong></p><h2>Released Laboratory Results</h2><div class="patient-lab-results">{result_list}</div></section>""",
    )


@app.post("/portal/recover", response_class=HTMLResponse)
def recover_patient_id(email: str = Form(...), contact_number: str = Form(...)):
    normalized_contact = normalize_philippine_number(contact_number)
    if normalized_contact is None:
        return page("Recovery Error", '<section class="card error"><h1>Unable to retrieve Patient ID</h1><p>Enter a valid Philippine mobile number.</p><a class="button" href="/portal/recover">Try Again</a></section>')
    with connect_db() as connection:
        legacy_local = f"0{normalized_contact[3:]}"
        legacy_international = normalized_contact[1:]
        patient = connection.execute(
            """SELECT patient_id, email, portal_activated FROM patients
            WHERE email = ? AND contact_number IN (?, ?, ?)""",
            (email.strip().lower(), normalized_contact, legacy_local, legacy_international),
        ).fetchone()
    if patient is None:
        return page("Recovery Error", '<section class="card error"><h1>Unable to retrieve Patient ID</h1><p>The details did not match an active patient portal.</p><a class="button" href="/portal/recover">Try Again</a></section>')

    otp_id = secrets.token_urlsafe(18)
    otp_code = f"{secrets.randbelow(1_000_000):06d}"
    with connect_db() as connection:
        connection.execute(
            "INSERT INTO recovery_otps (otp_id, patient_id, code_hash, expires_at) VALUES (?, ?, ?, ?)",
            (otp_id, patient["patient_id"], token_hash(otp_code), datetime.now(timezone.utc).timestamp() + 600),
        )
    delivery_status = send_email_message(
        patient["email"],
        "GAPCC Patient ID recovery code",
        f"Your GAPCC Patient ID recovery code is {otp_code}. It expires in 10 minutes.",
    )
    if delivery_status != "Sent":
        detail = "Outlook email delivery is not configured." if delivery_status == "Not Configured" else "Outlook could not deliver the recovery email."
        return page("Recovery Delivery Error", f'<section class="card error"><h1>OTP could not be delivered</h1><p>{escape(detail)}</p><p class="muted">Ask the clinic administrator to verify the Outlook SMTP settings, then request a new code.</p><a class="button" href="/portal/recover">Try Again</a></section>')
    return page(
        "Confirm Identity",
        f"""<section class="card"><h1>Confirm Your Identity</h1>
        <p>We sent a six-digit one-time password to {escape(patient['email'])}.</p>
        <p class="muted">The code expires in 10 minutes. Check your spam or junk folder if you do not see it.</p>
        <form action="/portal/recover/verify" method="post"><input type="hidden" name="otp_id" value="{escape(otp_id, quote=True)}"><label for="otp_code">One-Time Password</label><input id="otp_code" name="otp_code" inputmode="numeric" maxlength="6" required><button class="button green" type="submit">Confirm and Retrieve ID</button></form></section>""",
    )


@app.post("/portal/recover/verify", response_class=HTMLResponse)
def verify_recovery_otp(otp_id: str = Form(...), otp_code: str = Form(...)):
    with connect_db() as connection:
        otp = connection.execute(
            "SELECT * FROM recovery_otps WHERE otp_id = ? AND used = 0", (otp_id,)
        ).fetchone()
        if otp is None or float(otp["expires_at"]) < datetime.now(timezone.utc).timestamp() or otp["attempts"] >= 5:
            return page("OTP Error", '<section class="card error"><h1>OTP expired or unavailable</h1><p>Start the recovery process again.</p><a class="button" href="/portal/recover">Try Again</a></section>')
        if token_hash(otp_code.strip()) != otp["code_hash"]:
            connection.execute("UPDATE recovery_otps SET attempts = attempts + 1 WHERE otp_id = ?", (otp_id,))
            return page("OTP Error", '<section class="card error"><h1>Incorrect OTP</h1><p>Check the code and try again.</p><a class="button" href="/portal/recover">Request a new OTP</a></section>')
        connection.execute("UPDATE recovery_otps SET used = 1 WHERE otp_id = ?", (otp_id,))
        patient = connection.execute("SELECT * FROM patients WHERE patient_id = ?", (otp["patient_id"],)).fetchone()
    if not patient["portal_activated"]:
        activation_token = secrets.token_urlsafe(32)
        with connect_db() as connection:
            connection.execute("UPDATE patients SET portal_token_hash = ? WHERE patient_id = ?", (token_hash(activation_token), patient["patient_id"]))
        activation_url = f"/activate/{activation_token}"
        return page(
            "Activate Patient Portal",
            f"<section class=\"card\"><h1>Portal Found, Activation Needed</h1><div class=\"notice\"><strong>Your Patient ID:</strong> {escape(patient['patient_id'])}</div><p class=\"muted\">Use the secure activation link sent to your email to finish setting up your portal.</p><a class=\"button green\" href=\"{activation_url}\">Activate Portal</a><a class=\"entry-home\" href=\"/\">Back to GAPCC Home</a></section>",
            page_class="entry-shell entry-patient entry-booking-required",
        )
    return page("Patient ID Retrieved", f"<section class=\"card\"><h1>Patient ID Retrieved</h1><div class=\"notice\"><strong>Your Patient ID:</strong> {escape(patient['patient_id'])}</div><a class=\"button green\" href=\"/portal\">Open Existing Patient Portal</a><a class=\"entry-home\" href=\"/\">Back to GAPCC Home</a></section>", page_class="entry-shell entry-patient entry-booking-required")


@app.post("/portal", response_class=HTMLResponse)
def open_portal(
    patient_id: str = Form(...),
    email: str = Form(...),
    contact_number: str = Form(...),
    notice: str = "",
):
    normalized_contact = normalize_philippine_number(contact_number)
    if normalized_contact is None:
        return portal_form("Enter a valid Philippine mobile number.")
    with connect_db() as connection:
        legacy_local = f"0{normalized_contact[3:]}"
        legacy_international = normalized_contact[1:]
        patient = connection.execute(
            """SELECT * FROM patients
            WHERE patient_id = ? AND email = ? AND contact_number IN (?, ?, ?) AND portal_activated = 1""",
            (patient_id.strip(), email.strip().lower(), normalized_contact, legacy_local, legacy_international),
        ).fetchone()
        if patient is None:
            return portal_form("The details did not match an active patient portal.")
        bookings = connection.execute(
            """SELECT b.booking_id, b.token_id, b.category, b.service, b.facility, b.appointment_date, b.appointment_time,
            b.status, b.doctor_id, b.confirmation_acknowledged_at, d.display_name AS doctor_name
            FROM bookings b LEFT JOIN doctors d ON d.doctor_id = b.doctor_id
            WHERE b.patient_id = ? ORDER BY b.booking_id DESC""",
            (patient["patient_id"],),
        ).fetchall()
        booking_doctors = connection.execute(
            "SELECT doctor_id, display_name, specialization FROM doctors WHERE active = 1 ORDER BY display_name"
        ).fetchall()
        results = connection.execute(
            """SELECT result_id, test_name, result_summary, result_status, collected_at, released_at, pdf_filename,
            pdf_data IS NOT NULL AS has_document,
            d.display_name AS doctor_name
            FROM lab_results l LEFT JOIN doctors d ON d.doctor_id = l.doctor_id
            WHERE patient_id = ? AND result_status = 'Released'
            ORDER BY collected_at DESC, result_id DESC""",
            (patient["patient_id"],),
        ).fetchall()
        records = connection.execute(
            """SELECT r.record_type, r.note, r.created_at, d.display_name AS doctor_name
            FROM patient_records r LEFT JOIN doctors d ON d.doctor_id = r.doctor_id
            WHERE r.patient_id = ? ORDER BY r.record_id DESC""",
            (patient["patient_id"],),
        ).fetchall()

    creds = (
        f'<input type="hidden" name="patient_id" value="{escape(patient["patient_id"], quote=True)}">'
        f'<input type="hidden" name="email" value="{escape(patient["email"], quote=True)}">'
        f'<input type="hidden" name="contact_number" value="{escape(patient["contact_number"], quote=True)}">'
    )
    booking_rows = ""
    today = datetime.now(timezone.utc).date().isoformat()
    for booking in bookings:
        acknowledgment = "Acknowledged"
        if not booking["confirmation_acknowledged_at"]:
            acknowledgment = (
                f'<form action="/portal/appointments/acknowledge" method="post">{creds}'
                f'<input type="hidden" name="booking_id" value="{booking["booking_id"]}">'
                '<button class="button green" type="submit">Confirm received</button></form>'
            )
        actions = "-"
        if booking["status"] in {"Scheduled", "Confirmed"} and booking["appointment_date"] >= today:
            reschedule_doctors = "".join(
                f'<option value="{escape(doctor["doctor_id"], quote=True)}" {"selected" if doctor["doctor_id"] == booking["doctor_id"] else ""}>{escape(doctor["display_name"])} - {escape(doctor["specialization"])}</option>'
                for doctor in booking_doctors
            )
            reschedule_times = "".join(
                f'<option value="{escape(value, quote=True)}" {"selected" if value == booking["appointment_time"] else ""}>{escape(value)}</option>'
                for value in APPOINTMENT_TIME_OPTIONS
            )
            actions = (
                f'<form class="appointment-reschedule-form" action="/portal/appointments/reschedule" method="post" data-slot-availability-form>{creds}'
                f'<input type="hidden" name="booking_id" value="{booking["booking_id"]}"><input type="hidden" data-slot-exclude value="{booking["booking_id"]}">'
                f'<label for="reschedule-doctor-{booking["booking_id"]}">Doctor</label><select id="reschedule-doctor-{booking["booking_id"]}" name="doctor_id" data-slot-doctor required><option value="">Choose a doctor</option>{reschedule_doctors}</select>'
                f'<label for="reschedule-date-{booking["booking_id"]}">Date</label><input id="reschedule-date-{booking["booking_id"]}" type="date" name="appointment_date" data-slot-date min="{today}" value="{escape(booking["appointment_date"], quote=True)}" required>'
                f'<label for="reschedule-time-{booking["booking_id"]}">Clinic hours</label><select id="reschedule-time-{booking["booking_id"]}" name="appointment_time" data-slot-time required>{reschedule_times}</select>'
                '<p data-slot-availability role="status" aria-live="polite">Checking availability...</p>'
                '<button class="button" data-slot-submit type="submit" disabled>Reschedule</button></form>'
                f'<form action="/portal/appointments/cancel" method="post" onsubmit="return confirm(\'Cancel this appointment?\');">{creds}'
                f'<input type="hidden" name="booking_id" value="{booking["booking_id"]}"><button class="button" type="submit">Cancel appointment</button></form>'
            )
        booking_rows += (
            f"<tr><td>{escape(booking['appointment_date'])}</td><td>{escape(booking['token_id'])}</td><td>{escape(booking['service'])}</td>"
            f"<td>{escape(booking['facility'])}</td><td>{escape(booking['appointment_time'])}</td><td>{escape(booking['status'])}</td>"
            f"<td>{escape(booking['doctor_name'] or 'Not assigned yet')}</td><td>{acknowledgment}</td><td>{actions}</td></tr>"
        )
    booking_rows = booking_rows or '<tr><td colspan="9">No appointments have been booked yet.</td></tr>'
    patient_token = sign_access_token("patient", patient["patient_id"])
    result_rows = ""
    for result in results:
        document = (
            result_document_preview(result["result_id"], patient_token, result["test_name"])
            if result["has_document"]
            else '<p class="muted">No uploaded document is available for this result.</p>'
        )
        result_rows += (
            f'<article class="patient-lab-result"><h3>{escape(result["test_name"])}</h3>'
            f'<p>{escape(result["result_summary"])}</p><p class="muted">Conducted: {escape(result["collected_at"])} · '
            f'Doctor: {escape(result["doctor_name"] or "Not assigned")}</p>{document}</article>'
        )
    result_rows = result_rows or '<p class="muted">No released laboratory results yet.</p>'
    record_rows = "".join(
        f"<tr><td>{escape(record['created_at'])}</td><td>{escape(record['record_type'])}</td><td>{escape(record['note'])}</td><td>{escape(record['doctor_name'] or 'Not assigned')}</td></tr>"
        for record in records
    ) or '<tr><td colspan="4">No previous consultation records yet.</td></tr>'
    full_name = patient_name(patient)
    initial = escape(full_name[:1].upper() or "P")
    next_booking = bookings[0] if bookings else None
    next_text = f"{next_booking['appointment_date']} ? {next_booking['service']}" if next_booking else "No upcoming appointment"
    profile_picture = patient["profile_picture"]
    profile_avatar = (
        f'<img class="profile-avatar patient-profile-picture" src="data:image/jpeg;base64,{base64.b64encode(bytes(profile_picture)).decode("ascii")}" alt="Patient profile picture">'
        if profile_picture
        else f'<div class="profile-avatar" aria-hidden="true">{initial}</div>'
    )
    profile_card = (
        f'<section class="portal-profile" id="patient-profile-summary">{profile_avatar}<div class="profile-main">'
        '<div class="profile-heading"><p class="profile-label">Patient profile</p><span class="profile-badge">Active</span></div>'
        f'<h2>{escape(full_name)}</h2><div class="profile-grid">'
        f'<div class="profile-field"><span>Patient ID</span><strong>{escape(patient["patient_id"])}</strong></div>'
        f'<div class="profile-field profile-field-editable"><span>Email</span><strong>{escape(patient["email"])}</strong><a class="patient-profile-edit-link" href="#patient-profile">Edit</a></div>'
        f'<div class="profile-field profile-field-editable"><span>Contact</span><strong>{escape(patient["contact_number"])}</strong><a class="patient-profile-edit-link" href="#patient-profile">Edit</a></div>'
        f'<div class="profile-field"><span>Gender</span><strong>{escape(patient["gender"])}</strong></div>'
        f'<div class="profile-field"><span>Birthday</span><strong>{escape(patient["date_of_birth"])}</strong></div>'
        f'<div class="profile-field"><span>Age</span><strong>{patient_age(patient["date_of_birth"])}</strong></div>'
        '</div></div></section>'
    )
    profile_editor = (
        f'<section class="doctor-panel" id="patient-profile"><div class="panel-heading"><h2>Edit Profile</h2><small>Update contact details and photo</small></div>'
        f'<form class="patient-profile-edit" action="/portal/profile/update" method="post" enctype="multipart/form-data">'
        f'<input type="hidden" name="patient_id" value="{escape(patient["patient_id"], quote=True)}">'
        f'<input type="hidden" name="current_email" value="{escape(patient["email"], quote=True)}">'
        f'<input type="hidden" name="current_contact_number" value="{escape(patient["contact_number"], quote=True)}">'
        f'<div><label for="profile-email">Email</label><input id="profile-email" type="email" name="email" value="{escape(patient["email"], quote=True)}" autocomplete="email" required></div>'
        f'<div><label for="profile-contact">Contact number</label><input id="profile-contact" type="tel" name="contact_number" value="{escape(patient["contact_number"], quote=True)}" autocomplete="tel" required></div>'
        '<div class="profile-picture-field"><label for="profile-picture">Profile picture</label><input id="profile-picture" type="file" name="profile_picture" accept="image/png,image/jpeg"><small>Optional. PNG or JPEG, up to 5 MB.</small></div>'
        '<button class="button green" type="submit">Save Changes</button></form></section>'
    )
    booking_form = patient_booking_form(patient["patient_id"], booking_doctors, "portal-booking")
    profile_notice = f'<p class="notice profile-save-notice" role="status">{escape(notice)}</p>' if notice else ""
    return set_portal_session(page(
        "My Patient Portal",
        f"""<style>{DOCTOR_DASHBOARD_CSS}{RESULT_ACTION_CSS}
.gapcc-view-hidden {{ display:none !important; }}
.doctor-nav a.is-active {{ background:#e9f6f3; color:#087e7a; box-shadow:inset 3px 0 0 #087e7a; }}
.doctor-home-link.sidebar-logout {{ color:#b42318; font-weight:800; }}
.doctor-panel table form {{ margin:0; }} .doctor-panel table .button {{ margin:0; width:auto; padding:7px 10px; font-size:11px; }}
.doctor-panel table {{ min-width:0; }}
.appointment-reschedule-form {{ display:grid; min-width:220px; gap:4px; margin-bottom:8px !important; }}
.appointment-reschedule-form [data-slot-availability] {{ margin:4px 0; color:#537074; font-size:10px; }}
.patient-book {{ display:inline-block; width:auto !important; margin-top:14px; text-decoration:none; text-align:center; }}
.patient-profile-picture {{ display:block; object-fit:cover; }}
.patient-profile-edit {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; }}
.patient-profile-edit .profile-picture-field,.patient-profile-edit button {{ grid-column:1/-1; }}
.patient-profile-edit small {{ display:block; margin-top:5px; color:#637d84; }}
.profile-save-notice {{ margin:0 0 14px; }}
.patient-lab-results {{ display:grid; gap:22px; }}
.patient-lab-result {{ padding:0 0 22px; border-bottom:1px solid #d9e8eb; }}
.patient-lab-result h3 {{ margin:12px 0 6px; }}
.patient-result-preview {{ display:block; width:100%; height:760px; border:1px solid #cbd9dc; border-radius:8px; background:#eef4f5; }}
.patient-result-actions {{ margin:8px 0; }}
.profile-field-editable {{ display:grid; grid-template-columns:minmax(0,1fr) auto; align-items:center; column-gap:8px; }}
.profile-field-editable span {{ grid-column:1/-1; }}
.patient-profile-edit-link {{ padding:4px 8px; border:1px solid #0b7776; border-radius:5px; color:#0b7776; font-size:11px; font-weight:700; text-decoration:none; }}
.patient-profile-edit-link:hover {{ color:#fff; background:#0b7776; }}
@media(max-width:600px){{.patient-profile-edit{{grid-template-columns:1fr}}.patient-profile-edit .profile-picture-field,.patient-profile-edit button{{grid-column:auto}}}}
@media(max-width:700px){{.patient-result-preview{{height:560px}}}}
</style><div class="doctor-dashboard patient-dashboard"><aside class="doctor-sidebar"><a class="doctor-brand" href="#patient-dashboard"><span class="doctor-brand-mark">+</span>GAPCC Patient</a>{profile_card}<nav class="doctor-nav" aria-label="Patient portal"><a href="#patient-dashboard">Dashboard</a><a href="#patient-appointments">Appointments</a><a href="#patient-results">Lab Results</a><a href="#patient-records">Consultation Records</a></nav><a class="doctor-home-link sidebar-logout" href="/portal/logout">&#x23FB; Log Out</a></aside>
<div class="doctor-dashboard-main"><header class="doctor-topbar"><p>Patient workspace / Dashboard</p><strong>{escape(patient['patient_id'])}</strong></header>
<div id="patient-dashboard">
<section class="doctor-welcome"><div><p class="eyebrow">Patient dashboard</p><h1>Welcome, {escape(full_name)}</h1><p>Your appointments, laboratory results, and consultation records in one place.</p></div><div class="welcome-schedule"><strong>Latest appointment</strong><br>{escape(next_text)}</div></section>
<div class="doctor-stats" id="patient-stats"><article class="doctor-stat"><span class="stat-icon">A</span><div><strong>{len(bookings)}</strong><span>Appointments</span></div></article><article class="doctor-stat"><span class="stat-icon">L</span><div><strong>{len(results)}</strong><span>Released lab results</span></div></article><article class="doctor-stat"><span class="stat-icon">R</span><div><strong>{len(records)}</strong><span>Consultation records</span></div></article></div>
{profile_notice}
<section class="doctor-panel" id="patient-booking"><div class="panel-heading"><h2>Book Another Appointment</h2><small>Check availability and reserve a slot</small></div>{booking_form}</section>
</div>
<section class="doctor-panel" id="patient-appointments"><div class="panel-heading"><h2>Appointments</h2><small>{len(bookings)} total</small></div><div class="table-scroll"><table><thead><tr><th>Date</th><th>Confirmation #</th><th>Service</th><th>Facility</th><th>Hours</th><th>Status</th><th>Doctor</th><th>Confirmation</th><th>Manage appointment</th></tr></thead><tbody>{booking_rows}</tbody></table></div><a class="button patient-book" href="#patient-dashboard">Book Another Appointment</a></section>
{profile_editor}
<section class="doctor-panel" id="patient-results"><div class="panel-heading"><h2>Laboratory Results</h2><small>{len(results)} released · newest first</small></div><div class="patient-lab-results">{result_rows}</div></section>
<section class="doctor-panel" id="patient-records"><div class="panel-heading"><h2>Consultation Records</h2><small>{len(records)} records</small></div><div class="table-scroll"><table><thead><tr><th>Date</th><th>Type</th><th>Note</th><th>Doctor</th></tr></thead><tbody>{record_rows}</tbody></table></div></section>
</div></div>
<script>{BOOKING_AVAILABILITY_SCRIPT}</script>
<script>window.addEventListener('load',function(){{var links=[].slice.call(document.querySelectorAll('.doctor-nav a[href^="#"]'));var views={{}};links.forEach(function(l){{var id=l.getAttribute('href').slice(1);views[id]=[document.getElementById(id)];}});views['patient-profile']=[document.getElementById('patient-profile')];function show(id){{if(!views[id])id='patient-dashboard';Object.keys(views).forEach(function(k){{views[k].forEach(function(n){{n.classList.toggle('gapcc-view-hidden',k!==id);}});}});links.forEach(function(l){{var on=l.getAttribute('href')==='#'+id;l.classList.toggle('is-active',on);if(on){{var c=document.querySelector('.doctor-topbar p');if(c)c.textContent='Patient workspace / '+l.textContent.trim();}}}});if(id==='patient-profile'){{var c=document.querySelector('.doctor-topbar p');if(c)c.textContent='Patient workspace / Edit Profile';}}window.scrollTo({{top:0}});}}document.addEventListener('click',function(e){{var l=e.target.closest('.doctor-nav a[href^="#"], .doctor-brand[href^="#"], .patient-book, .patient-profile-edit-link');if(!l)return;e.preventDefault();var id=l.getAttribute('href').slice(1);history.replaceState(null,'','#'+id);show(id);}});window.addEventListener('hashchange',function(){{show(location.hash.slice(1));}});show(location.hash.slice(1));}});</script>""",
        page_class="portal-dashboard",
    ), "patient", patient["patient_id"])


@app.post("/portal/appointments/acknowledge", response_class=HTMLResponse)
def acknowledge_appointment(patient_id: str = Form(...), email: str = Form(...), contact_number: str = Form(...), booking_id: int = Form(...)):
    normalized_contact = normalize_philippine_number(contact_number)
    if normalized_contact is None:
        return portal_form("Enter a valid Philippine mobile number.")
    with connect_db() as connection:
        patient = connection.execute("SELECT * FROM patients WHERE patient_id = ? AND email = ? AND contact_number IN (?, ?, ?) AND portal_activated = 1", (patient_id.strip(), email.strip().lower(), normalized_contact, f"0{normalized_contact[3:]}", normalized_contact[1:])).fetchone()
        booking = connection.execute("SELECT booking_id FROM bookings WHERE booking_id = ? AND patient_id = ?", (booking_id, patient_id.strip())).fetchone() if patient else None
        if patient is None or booking is None:
            return portal_form("We could not verify that appointment confirmation.")
        connection.execute("UPDATE bookings SET confirmation_acknowledged_at = ? WHERE booking_id = ?", (utc_now(), booking_id))
    return open_portal(patient_id, email, contact_number)


@app.post("/portal/search", response_class=HTMLResponse)
def patient_search(patient_id: str = Form(...), email: str = Form(...), contact_number: str = Form(...), q: str = Form(...)):
    normalized_contact = normalize_philippine_number(contact_number)
    query = q.strip()
    with connect_db() as connection:
        patient = connection.execute("SELECT * FROM patients WHERE patient_id = ? AND email = ? AND contact_number = ? AND portal_activated = 1", (patient_id.strip(), email.strip().lower(), normalized_contact)).fetchone() if normalized_contact else None
        if patient is None:
            return portal_form("Your portal could not be verified for record search.")
        like = f"%{query}%"
        bookings = connection.execute("SELECT token_id, service, facility, appointment_date FROM bookings WHERE patient_id = ? AND (token_id LIKE ? OR service LIKE ? OR facility LIKE ? OR appointment_date LIKE ?) ORDER BY booking_id DESC", (patient_id, like, like, like, like)).fetchall()
        results = connection.execute("SELECT result_id, test_name, result_summary, result_status, collected_at, pdf_filename FROM lab_results WHERE patient_id = ? AND result_status = 'Released' AND (test_name LIKE ? OR result_summary LIKE ? OR collected_at LIKE ?) ORDER BY collected_at DESC, result_id DESC", (patient_id, like, like, like)).fetchall()
    booking_items = "".join(f"<li><strong>{escape(row['token_id'])}</strong> - {escape(row['service'])}, {escape(row['facility'])}, {escape(row['appointment_date'])}</li>" for row in bookings) or "<li>No matching appointments.</li>"
    result_items = "".join(f"<li><strong>{escape(row['test_name'])}</strong> - {escape(row['result_summary'])} ({escape(row['collected_at'])}){' ' + result_action_links(row['result_id'], sign_access_token('patient', patient['patient_id'])) if row['pdf_filename'] else ''}</li>" for row in results) or "<li>No matching released results.</li>"
    return page("My Record Search", f"<section class=\"card\"><h1>My Record Search</h1><p>Results for <strong>{escape(query)}</strong></p><h2>Appointments</h2><ul>{booking_items}</ul><h2>Laboratory Results</h2><ul>{result_items}</ul><a class=\"button\" href=\"/portal\">Back to Patient Portal</a></section>", page_class="portal-dashboard")


@app.post("/portal/profile/update", response_class=HTMLResponse)
async def update_patient_profile(
    patient_id: str = Form(...),
    current_email: str = Form(...),
    current_contact_number: str = Form(...),
    email: str = Form(...),
    contact_number: str = Form(...),
    profile_picture: Annotated[UploadFile | None, File()] = None,
):
    new_email = email.strip().lower()
    new_contact = normalize_philippine_number(contact_number)
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", new_email):
        return page("Profile Update Error", '<section class="card error"><h1>Invalid email address</h1><p>Enter a valid email address and try again.</p><a class="button" href="/portal">Return to Patient Portal</a></section>')
    if new_contact is None:
        return page("Profile Update Error", '<section class="card error"><h1>Invalid contact number</h1><p>Enter a valid Philippine mobile number and try again.</p><a class="button" href="/portal">Return to Patient Portal</a></section>')

    picture_data = None
    if profile_picture is not None and profile_picture.filename:
        upload = await profile_picture.read(MAX_PROFILE_PICTURE_BYTES + 1)
        if len(upload) > MAX_PROFILE_PICTURE_BYTES:
            return page("Profile Update Error", '<section class="card error"><h1>Profile picture is too large</h1><p>Choose a PNG or JPEG image smaller than 5 MB.</p><a class="button" href="/portal">Return to Patient Portal</a></section>')
        from PIL import Image, ImageOps

        try:
            with Image.open(io.BytesIO(upload)) as image:
                if image.format not in {"JPEG", "PNG"} or image.width * image.height > 20_000_000:
                    raise ValueError("Unsupported profile picture.")
                image.verify()
            with Image.open(io.BytesIO(upload)) as image:
                image = ImageOps.exif_transpose(image)
                image.thumbnail((512, 512))
                if image.mode in {"RGBA", "LA"} or "transparency" in image.info:
                    rgba = image.convert("RGBA")
                    background = Image.new("RGB", rgba.size, "white")
                    background.paste(rgba, mask=rgba.getchannel("A"))
                    image = background
                else:
                    image = image.convert("RGB")
                output = io.BytesIO()
                image.save(output, format="JPEG", quality=85, optimize=True)
                picture_data = output.getvalue()
        except (OSError, ValueError, SyntaxError, Image.DecompressionBombError):
            return page("Profile Update Error", '<section class="card error"><h1>Profile picture could not be used</h1><p>Upload a valid PNG or JPEG image.</p><a class="button" href="/portal">Return to Patient Portal</a></section>')

    with connect_db() as connection:
        connection.execute("BEGIN IMMEDIATE")
        patient = patient_for_portal_credentials(
            connection, patient_id, current_email, current_contact_number
        )
        if patient is None:
            return page("Profile Update Error", '<section class="card error"><h1>Profile could not be verified</h1><p>Sign in to the patient portal again before updating your profile.</p><a class="button" href="/portal">Return to Patient Portal</a></section>')
        duplicate = connection.execute(
            "SELECT 1 FROM patients WHERE email = ? AND patient_id != ?",
            (new_email, patient["patient_id"]),
        ).fetchone()
        if duplicate:
            return page("Profile Update Error", '<section class="card error"><h1>Email is already in use</h1><p>Use another email address and try again.</p><a class="button" href="/portal">Return to Patient Portal</a></section>')
        if picture_data is None:
            connection.execute(
                "UPDATE patients SET email = ?, contact_number = ? WHERE patient_id = ?",
                (new_email, new_contact, patient["patient_id"]),
            )
        else:
            connection.execute(
                "UPDATE patients SET email = ?, contact_number = ?, profile_picture = ? WHERE patient_id = ?",
                (new_email, new_contact, picture_data, patient["patient_id"]),
            )
    return open_portal(
        patient_id=patient_id,
        email=new_email,
        contact_number=new_contact,
        notice="Profile updated successfully.",
    )


@app.post("/register", response_class=HTMLResponse)
def register_patient(
    last_name: str = Form(...),
    given_name: str = Form(...),
    middle_name: str = Form(""),
    gender: str = Form(...),
    date_of_birth: str = Form(...),
    email: str = Form(...),
    contact_number: str = Form(...),
):
    if gender not in GENDER_OPTIONS:
        return registration_form("Please select a valid gender option.")
    try:
        birth_date = datetime.strptime(date_of_birth, "%Y-%m-%d").replace(tzinfo=timezone.utc).date()
    except ValueError:
        return registration_form("Please provide a valid birthday.")
    if birth_date > datetime.now(timezone.utc).date():
        return registration_form("Birthday cannot be in the future.")
    normalized_contact = normalize_philippine_number(contact_number)
    if normalized_contact is None:
        return registration_form("Enter a valid Philippine mobile number, such as 9171234567.")
    patient_id = f"GAPCC-P-{secrets.token_hex(6).upper()}"
    activation_token = secrets.token_urlsafe(32)
    try:
        with connect_db() as connection:
            connection.execute(
                """INSERT INTO patients
                (patient_id, last_name, given_name, middle_name, gender, date_of_birth, email, contact_number, portal_token_hash, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (patient_id, last_name.strip(), given_name.strip(), middle_name.strip(), gender, date_of_birth, email.strip().lower(), normalized_contact, token_hash(activation_token), utc_now()),
            )
    except sqlite3.IntegrityError:
        return registration_form("An account already exists for this email address. Please use the existing portal account.")

    activation_url = f"/activate/{activation_token}"
    return page(
        "Portal Created",
        f"""<section class="card"><h1>Step 2: Portal Initialized</h1>
        <div class="notice"><strong>Patient ID:</strong> {escape(patient_id)}<br><strong>Gender:</strong> {escape(gender)}<br><strong>Birthday:</strong> {escape(date_of_birth)}<br><strong>Age:</strong> {patient_age(date_of_birth)}<br>
        A secure activation link has been generated for {escape(email)}.</div>
        <p>Activate the portal using the secure link sent to your email, then continue to booking. Your patient ID will be retained automatically.</p>
        <a class="button green" href="{activation_url}">Activate Portal</a><a class="entry-home" href="/">Back to GAPCC Home</a></section>""",
        page_class="entry-shell entry-patient entry-booking-required",
    )


@app.get("/activate/{token}", response_class=HTMLResponse)
def activate_portal(token: str):
    with connect_db() as connection:
        patient = connection.execute(
            "SELECT patient_id FROM patients WHERE portal_token_hash = ?", (token_hash(token),)
        ).fetchone()
        if patient is None:
            return page("Activation Error", '<section class="card"><h1>Invalid activation link</h1><p>Please request a new portal registration.</p></section>')
        connection.execute("UPDATE patients SET portal_activated = 1, portal_token_hash = '' WHERE patient_id = ?", (patient["patient_id"],))
    return RedirectResponse(url="/", status_code=303)


@app.get("/availability", response_class=JSONResponse)
def appointment_availability(
    doctor_id: str = Query(...),
    appointment_date: str = Query(...),
    appointment_time: str = Query(...),
    exclude_booking_id: int | None = Query(None, ge=1),
):
    selected_date = date_string_to_date(appointment_date)
    if selected_date is None or selected_date < datetime.now(timezone.utc).date():
        return JSONResponse({"detail": "Choose a valid current or future appointment date."}, status_code=400)
    if appointment_time not in APPOINTMENT_TIME_OPTIONS:
        return JSONResponse({"detail": "Choose a valid clinic service time."}, status_code=400)
    with connect_db() as connection:
        doctor = connection.execute(
            "SELECT doctor_id FROM doctors WHERE doctor_id = ? AND active = 1",
            (doctor_id,),
        ).fetchone()
        if doctor is None:
            return JSONResponse({"detail": "Choose an active doctor."}, status_code=400)
        booked = appointment_slot_count(
            connection, doctor_id, appointment_date, appointment_time, exclude_booking_id
        )
    remaining = max(0, APPOINTMENT_CAPACITY - booked)
    return JSONResponse(
        {"capacity": APPOINTMENT_CAPACITY, "booked": booked, "remaining": remaining, "available": remaining > 0}
    )


def patient_booking_form(patient_id, doctors, form_id):
    doctor_options = "".join(
        f'<option value="{escape(doctor["doctor_id"], quote=True)}">{escape(doctor["display_name"])} - {escape(doctor["specialization"])}</option>'
        for doctor in doctors
    )
    time_options = "".join(
        f'<option value="{escape(appointment_time, quote=True)}">{escape(appointment_time)}</option>'
        for appointment_time in APPOINTMENT_TIME_OPTIONS
    )
    category_id = f"{form_id}-category"
    service_id = f"{form_id}-service"
    return f"""<form id="{form_id}" action="/book" method="post" data-slot-availability-form>
        <input type="hidden" name="patient_id" value="{escape(patient_id, quote=True)}">
        <div class="grid">
        <div><label for="{category_id}">Department</label><select id="{category_id}" name="category" data-booking-category><option value="Clinical">Clinical Care</option><option value="Imaging">Diagnostic Imaging</option></select></div>
        <div><label for="{service_id}">Service</label><select id="{service_id}" name="service" data-booking-service>{option_markup(CLINICAL_SERVICES)}</select></div>
        <div><label for="{form_id}-facility">Facility</label><select id="{form_id}-facility" name="facility">{option_markup(FACILITIES)}</select></div>
        <div><label for="{form_id}-doctor">Doctor</label><select id="{form_id}-doctor" name="doctor_id" data-slot-doctor required><option value="">Choose a doctor</option>{doctor_options}</select></div>
        <div><label for="{form_id}-date">Appointment Date</label><input id="{form_id}-date" type="date" name="appointment_date" data-slot-date min="{datetime.now(timezone.utc).date().isoformat()}" required></div>
        <div><label for="{form_id}-time">Clinic Service Hours</label><select id="{form_id}-time" name="appointment_time" data-slot-time required>{time_options}</select></div>
        </div><p data-slot-availability role="status" aria-live="polite">Choose a doctor and date to check live availability.</p>
        <button class="button" data-slot-submit type="submit" disabled>Confirm Appointment</button></form>
        <script>(function(){{const form=document.getElementById({form_id!r});const category=form.querySelector('[data-booking-category]');const service=form.querySelector('[data-booking-service]');const clinical={option_markup(CLINICAL_SERVICES)!r};const imaging={option_markup(DIAGNOSTIC_IMAGING)!r};category.addEventListener('change',function(){{service.innerHTML=category.value==='Imaging'?imaging:clinical;}});}})();</script>"""


@app.get("/book", response_class=HTMLResponse)
def booking_page(patient_id: str = Query(...)):
    with connect_db() as connection:
        patient = connection.execute("SELECT * FROM patients WHERE patient_id = ?", (patient_id,)).fetchone()
        doctors = connection.execute(
            "SELECT doctor_id, display_name, specialization FROM doctors WHERE active = 1 ORDER BY display_name"
        ).fetchall()
    if patient is None or not patient["portal_activated"]:
        return page("Portal Required", '<section class="card"><h1>Activate your portal first</h1><p>Complete pre-registration and portal activation before booking.</p><a class="button" href="/register">Start Pre-Registration</a><a class="entry-home" href="/">Back to GAPCC Home</a></section>', page_class="entry-shell entry-booking entry-booking-required")
    booking_form = patient_booking_form(patient["patient_id"], doctors, "standalone-booking")
    return page(
        "Book Appointment",
        f"""<section class="card"><h1>Step 3: Booking Handshake</h1>
        <div class="notice"><strong>Patient:</strong> {escape(patient_name(patient))}<br><strong>Gender:</strong> {escape(patient['gender'])}<br><strong>Birthday:</strong> {escape(patient['date_of_birth'])}<br><strong>Age:</strong> {patient_age(patient['date_of_birth'])}<br><strong>Patient ID:</strong> {escape(patient['patient_id'])}<br>Your identity is retained; no duplicate demographic entry is required.</div>
        {booking_form}</section><script>{BOOKING_AVAILABILITY_SCRIPT}</script>""",
    )


@app.post("/book", response_class=HTMLResponse)
def create_booking(
    patient_id: str = Form(...),
    category: str = Form(...),
    service: str = Form(...),
    facility: str = Form(...),
    doctor_id: str = Form(...),
    appointment_date: str = Form(...),
    appointment_time: str = Form(...),
):
    services = CLINICAL_SERVICES if category == "Clinical" else DIAGNOSTIC_IMAGING
    if category not in {"Clinical", "Imaging"} or service not in services or facility not in FACILITIES:
        return page("Booking Error", '<section class="card error"><h1>Invalid booking details</h1><p>Please return to the booking step and choose valid options.</p></section>')
    selected_date = date_string_to_date(appointment_date)
    if selected_date is None:
        return page("Booking Error", '<section class="card error"><h1>Invalid appointment date</h1><p>Please choose a valid date in YYYY-MM-DD format.</p></section>')
    if selected_date < datetime.now(timezone.utc).date():
        return page("Booking Error", '<section class="card error"><h1>Appointment date cannot be in the past</h1><p>Please choose a future or current date.</p></section>')
    if appointment_time not in APPOINTMENT_TIME_OPTIONS:
        return page("Booking Error", '<section class="card error"><h1>Invalid clinic service hours</h1><p>Choose one of the available clinic operating-hour options.</p></section>')
    with connect_db() as connection:
        connection.execute("BEGIN IMMEDIATE")
        patient = connection.execute("SELECT patient_id, email, contact_number FROM patients WHERE patient_id = ? AND portal_activated = 1", (patient_id,)).fetchone()
        if patient is None:
            return page("Booking Error", '<section class="card error"><h1>Patient portal not active</h1><p>Please activate the portal before booking.</p></section>')
        doctor = connection.execute(
            "SELECT doctor_id, display_name FROM doctors WHERE doctor_id = ? AND active = 1",
            (doctor_id,),
        ).fetchone()
        if doctor is None:
            return page("Booking Error", '<section class="card error"><h1>Doctor unavailable</h1><p>Choose an active doctor and try again.</p><a class="button" href="/book?patient_id=' + escape(patient_id, quote=True) + '">Return to booking</a></section>')
        if appointment_slot_count(connection, doctor_id, appointment_date, appointment_time) >= APPOINTMENT_CAPACITY:
            return page("Booking Error", f'<section class="card error"><h1>Appointment slot is full</h1><p>There are no remaining appointments for this doctor at the selected date and time. Choose another slot; capacity is {APPOINTMENT_CAPACITY} patients per doctor, date, and clinic-time period.</p><a class="button" href="/book?patient_id={escape(patient_id, quote=True)}">Choose another slot</a></section>')
        latest_token = connection.execute(
            "SELECT MAX(CAST(substr(token_id, 7) AS INTEGER)) AS latest FROM bookings"
        ).fetchone()["latest"] or 0
        token_id = f"GAPCC-{latest_token + 1:04d}"
        booking_cursor = connection.execute(
            """INSERT INTO bookings
            (token_id, patient_id, category, service, facility, appointment_date, appointment_time, doctor_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (token_id, patient_id, category, service, facility, appointment_date, appointment_time, doctor_id, utc_now()),
        )
        confirmation_status = queue_appointment_messages(connection, booking_cursor.lastrowid, patient, token_id, service, facility, appointment_date, appointment_time)
    delivery_message = {"Sent": f"booking confirmation sent to {patient['email']}", "Failed": f"booking confirmation could not be delivered to {patient['email']}", "Not Configured": "booking confirmation not sent because email delivery is not configured"}[confirmation_status]
    return open_portal(
        patient_id=patient["patient_id"],
        email=patient["email"],
        contact_number=patient["contact_number"],
        notice=f"Appointment request received. Your appointment token: {token_id}. {delivery_message}. The clinic will email you separately when it is approved and confirmed.",
    )


@app.post("/portal/appointments/reschedule", response_class=HTMLResponse)
def reschedule_patient_appointment(
    patient_id: str = Form(...),
    email: str = Form(...),
    contact_number: str = Form(...),
    booking_id: int = Form(...),
    doctor_id: str = Form(...),
    appointment_date: str = Form(...),
    appointment_time: str = Form(...),
):
    selected_date = date_string_to_date(appointment_date)
    if selected_date is None or selected_date < datetime.now(timezone.utc).date():
        return page("Reschedule Error", '<section class="card error"><h1>Invalid appointment date</h1><p>Choose a current or future date.</p><a class="button" href="/portal">Return to Patient Login</a></section>')
    if appointment_time not in APPOINTMENT_TIME_OPTIONS:
        return page("Reschedule Error", '<section class="card error"><h1>Invalid clinic service hours</h1><p>Choose an available clinic time period.</p><a class="button" href="/portal">Return to Patient Login</a></section>')
    with connect_db() as connection:
        connection.execute("BEGIN IMMEDIATE")
        patient = patient_for_portal_credentials(connection, patient_id, email, contact_number)
        booking = connection.execute(
            """SELECT b.booking_id, b.token_id, b.patient_id, b.service, b.facility, b.appointment_date,
            b.appointment_time, b.doctor_id, b.status, d.display_name AS doctor_name
            FROM bookings b LEFT JOIN doctors d ON d.doctor_id = b.doctor_id
            WHERE b.booking_id = ? AND b.patient_id = ?""",
            (booking_id, patient_id.strip()),
        ).fetchone() if patient else None
        doctor = connection.execute(
            "SELECT doctor_id, display_name FROM doctors WHERE doctor_id = ? AND active = 1",
            (doctor_id,),
        ).fetchone()
        if patient is None or booking is None or booking["status"] not in {"Scheduled", "Confirmed"} or doctor is None:
            return page("Reschedule Error", '<section class="card error"><h1>Appointment could not be rescheduled</h1><p>Verify the patient and appointment details, then try again.</p><a class="button" href="/portal">Return to Patient Login</a></section>')
        if appointment_slot_count(
            connection, doctor_id, appointment_date, appointment_time, booking_id
        ) >= APPOINTMENT_CAPACITY:
            return page("Reschedule Error", f'<section class="card error"><h1>Appointment slot is full</h1><p>This doctor/date/time has reached its capacity of {APPOINTMENT_CAPACITY}. Choose a different slot.</p><a class="button" href="/portal">Return to Patient Login</a></section>')
        connection.execute(
            """UPDATE bookings SET doctor_id = ?, appointment_date = ?, appointment_time = ?
            WHERE booking_id = ?""",
            (doctor_id, appointment_date, appointment_time, booking_id),
        )
        updated_booking = connection.execute(
            """SELECT b.booking_id, b.token_id, b.patient_id, b.service, b.facility,
            b.appointment_date, b.appointment_time FROM bookings b WHERE b.booking_id = ?""",
            (booking_id,),
        ).fetchone()
        notify_patient_booking_change(
            connection, updated_booking, patient, "rescheduled", doctor["display_name"]
        )
    return open_portal(patient_id, email, contact_number)


@app.post("/portal/appointments/cancel", response_class=HTMLResponse)
def cancel_patient_appointment(
    patient_id: str = Form(...),
    email: str = Form(...),
    contact_number: str = Form(...),
    booking_id: int = Form(...),
):
    with connect_db() as connection:
        connection.execute("BEGIN IMMEDIATE")
        patient = patient_for_portal_credentials(connection, patient_id, email, contact_number)
        booking = connection.execute(
            """SELECT b.booking_id, b.token_id, b.patient_id, b.service, b.facility, b.appointment_date,
            b.appointment_time, b.doctor_id, b.status, d.display_name AS doctor_name
            FROM bookings b LEFT JOIN doctors d ON d.doctor_id = b.doctor_id
            WHERE b.booking_id = ? AND b.patient_id = ?""",
            (booking_id, patient_id.strip()),
        ).fetchone() if patient else None
        if patient is None or booking is None or booking["status"] not in {"Scheduled", "Confirmed"}:
            return page("Cancellation Error", '<section class="card error"><h1>Appointment could not be cancelled</h1><p>Verify the patient and appointment details, then try again.</p><a class="button" href="/portal">Return to Patient Login</a></section>')
        connection.execute(
            "UPDATE bookings SET status = 'Cancelled' WHERE booking_id = ?",
            (booking_id,),
        )
        notify_patient_booking_change(
            connection, booking, patient, "cancelled", booking["doctor_name"] or "Unassigned"
        )
    return open_portal(patient_id, email, contact_number)


@app.get("/search", response_class=HTMLResponse)
def search_portal(q: str = Query("", max_length=80)):
    query = q.strip().casefold()
    if not query:
        return page("Search GAPCC", '<section class="card"><h1>Search GAPCC</h1><p>Search for a doctor, care service, or facility.</p><form class="site-search" action="/search" method="get"><input name="q" aria-label="Search query" placeholder="Try cardiology or ultrasound" required><button class="button" type="submit">Search</button></form></section>')

    services = [service for service in CLINICAL_SERVICES + DIAGNOSTIC_IMAGING if query in service.casefold()]
    facilities = [facility for facility in FACILITIES if query in facility.casefold()]
    with connect_db() as connection:
        doctors = connection.execute(
            """SELECT display_name, specialization, facility FROM doctors
            WHERE active = 1 AND (lower(display_name) LIKE ? OR lower(specialization) LIKE ? OR lower(facility) LIKE ?)
            ORDER BY display_name""",
            (f"%{query}%", f"%{query}%", f"%{query}%"),
        ).fetchall()

    service_items = "".join(f"<li><strong>{escape(service)}</strong> <span class=\"muted\">Care service</span></li>" for service in services)
    facility_items = "".join(f"<li><strong>{escape(facility)}</strong> <span class=\"muted\">Facility</span></li>" for facility in facilities)
    doctor_items = "".join(f"<li><strong>{escape(doctor['display_name'])}</strong> - {escape(doctor['specialization'])}<br><span class=\"muted\">{escape(doctor['facility'])}</span></li>" for doctor in doctors)
    result_count = len(services) + len(facilities) + len(doctors)
    empty = "<p class=\"muted\">No public care information matched that search.</p>" if result_count == 0 else ""
    return page(
        "Search GAPCC",
        f"""<section class="card"><h1>GAPCC Care Search</h1><p>Showing public care information matching <strong>{escape(q.strip())}</strong>.</p>{empty}
        <div class="grid"><div><h2>Doctors</h2><ul>{doctor_items or '<li>No matching doctors.</li>'}</ul></div><div><h2>Care Services</h2><ul>{service_items or '<li>No matching services.</li>'}</ul></div></div><h2>Facilities</h2><ul>{facility_items or '<li>No matching facilities.</li>'}</ul><p class="muted">For private patient records and laboratory results, use the secure patient, doctor, or admin workspace.</p></section>""",
    )


@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    token = request.cookies.get(PORTAL_SESSION_COOKIE, "")
    claim = verify_access_token(token)
    if claim and claim[0] == "admin":
        return admin_login("", "", session_token=token)
    return page(
        "Admin Database",
        """<section class="card"><h1>GAPCC Admin Console</h1><p>Manage the shared patient, doctor, and scheduling database.</p>
        <form action="/admin" method="post"><div class="grid"><div><label for="email">Admin Email</label><input id="email" type="email" name="email" value="admin@gapcc.local" autocomplete="username" required></div><div><label for="password">Password</label><input id="password" type="password" name="password" autocomplete="current-password" required></div></div><button class="button" type="submit">Log In to Admin Dashboard</button></form><p class="entry-signup"><a href="/account-recover/admin">Forgot password?</a></p><p class="entry-signup">Don't have an account? <a href="/account-request/admin">Create account</a> · <a href="/account-request/admin/status">Check request status</a></p><a class="entry-home" href="/">Back to GAPCC Home</a></section>""",
        page_class="entry-shell entry-admin",
    )


@app.get("/admin/logout")
def admin_portal_logout():
    response = RedirectResponse(url="/admin", status_code=303)
    return clear_portal_session(response)


def account_recovery_form(account_type, message=""):
    if account_type not in {"admin", "doctor"}:
        return HTMLResponse("Account type not found.", status_code=404)
    title = "Administrator" if account_type == "admin" else "Doctor"
    notice = f'<div class="notice error">{escape(message)}</div>' if message else ""
    return page(
        f"{title} Password Recovery",
        f"""<section class="card"><h1>{title} Password Recovery</h1><p>Enter your login username/email. We will send a one-time code to the email address registered for notifications.</p>{notice}<form action="/account-recover/{account_type}" method="post"><label for="email">Account Email</label><input id="email" type="email" name="email" autocomplete="email" required><button class="button" type="submit">Send Recovery Code</button></form><a class="entry-home" href="/{account_type}">Back to Log In</a></section>""",
        page_class=f"entry-shell entry-{account_type}",
    )


@app.get("/account-recover/{account_type}", response_class=HTMLResponse)
def account_recovery_page(account_type: str):
    return account_recovery_form(account_type)


@app.post("/account-recover/{account_type}", response_class=HTMLResponse)
def request_account_recovery(account_type: str, email: str = Form(...)):
    if account_type not in {"admin", "doctor"}:
        return HTMLResponse("Account type not found.", status_code=404)
    clean_email = email.strip().lower()
    table_name = "admins" if account_type == "admin" else "doctors"
    active_clause = "" if account_type == "admin" else " AND active = 1"
    with connect_db() as connection:
        account = connection.execute(
            f"SELECT {'admin_id' if account_type == 'admin' else 'doctor_id'} AS account_id, email, contact_email FROM {table_name} WHERE email = ?{active_clause}",
            (clean_email,),
        ).fetchone()
        if account is None:
            return account_recovery_form(account_type, "If an active account matches that email, a recovery code will be sent.")
        destination = notification_address(account)
        if not destination:
            return account_recovery_form(account_type, "No notification email is on file for this account. Contact the clinic administrator.")
        otp_id = secrets.token_urlsafe(18)
        otp_code = f"{secrets.randbelow(1_000_000):06d}"
        connection.execute(
            "INSERT INTO account_recovery_otps (otp_id, account_type, account_id, code_hash, expires_at) VALUES (?, ?, ?, ?, ?)",
            (otp_id, account_type, account["account_id"], token_hash(otp_code), datetime.now(timezone.utc).timestamp() + 600),
        )
    delivery_status = send_email_message(
        destination,
        "GAPCC staff account recovery code",
        f"Your GAPCC {account_type} account recovery code is {otp_code}. It expires in 10 minutes.",
    )
    if delivery_status != "Sent":
        return account_recovery_form(account_type, "The recovery email could not be delivered. Contact the clinic administrator to check email settings.")
    return page(
        "Verify Recovery Email",
        f"""<section class="card"><h1>Verify Your Email</h1><p>We sent a one-time code to {escape(destination)}. It expires in 10 minutes.</p><form action="/account-recover/{account_type}/verify" method="post"><input type="hidden" name="otp_id" value="{escape(otp_id, quote=True)}"><label for="otp_code">One-Time Code</label><input id="otp_code" name="otp_code" inputmode="numeric" maxlength="6" required><label for="password">New Password</label><input id="password" type="password" name="password" minlength="12" pattern="{PASSWORD_POLICY_PATTERN}" autocomplete="new-password" required><small>{PASSWORD_POLICY_MESSAGE}</small><button class="button" type="submit">Verify and Update Password</button></form></section>""",
        page_class=f"entry-shell entry-{account_type}",
    )


@app.post("/account-recover/{account_type}/verify", response_class=HTMLResponse)
def verify_account_recovery(account_type: str, otp_id: str = Form(...), otp_code: str = Form(...), password: str = Form(...)):
    if account_type not in {"admin", "doctor"}:
        return HTMLResponse("Account type not found.", status_code=404)
    if not password_meets_policy(password):
        return page("Password Recovery Error", f'<section class="card error"><h1>Password does not meet requirements</h1><p>{escape(PASSWORD_POLICY_MESSAGE)}</p><a class="button" href="/account-recover/{account_type}">Start Again</a></section>')
    with connect_db() as connection:
        otp = connection.execute(
            "SELECT * FROM account_recovery_otps WHERE otp_id = ? AND account_type = ? AND used = 0",
            (otp_id, account_type),
        ).fetchone()
        if otp is None or otp["expires_at"] < datetime.now(timezone.utc).timestamp() or otp["attempts"] >= 5:
            return account_recovery_form(account_type, "The code expired or is unavailable. Request a new code.")
        if token_hash(otp_code.strip()) != otp["code_hash"]:
            connection.execute("UPDATE account_recovery_otps SET attempts = attempts + 1 WHERE otp_id = ?", (otp_id,))
            return page("Recovery Code Error", f'<section class="card error"><h1>Incorrect recovery code</h1><p>Check the code and try again.</p><a class="button" href="/account-recover/{account_type}">Request a New Code</a></section>')
        table_name, id_column = ("admins", "admin_id") if account_type == "admin" else ("doctors", "doctor_id")
        connection.execute(f"UPDATE {table_name} SET password_hash = ? WHERE {id_column} = ?", (token_hash(password), otp["account_id"]))
        connection.execute("UPDATE account_recovery_otps SET used = 1 WHERE otp_id = ?", (otp_id,))
    return page("Password Updated", f'<section class="card"><h1>Password Updated</h1><p>Your {escape(account_type)} portal password has been changed.</p><a class="button" href="/{account_type}">Return to Log In</a></section>')


@app.post("/admin", response_class=HTMLResponse)
def admin_login(
    email: str = Form(""),
    password: str = Form(""),
    session_token: Annotated[str, Form()] = "",
):
    with connect_db() as connection:
        credential_token = session_token or password
        claim = verify_access_token(credential_token) if credential_token else None
        if claim:
            admin = connection.execute(
                "SELECT * FROM admins WHERE admin_id = ?",
                (claim[1],),
            ).fetchone() if claim[0] == "admin" else None
        else:
            admin = connection.execute(
                "SELECT * FROM admins WHERE email = ? AND password_hash = ?",
                (email.strip().lower(), token_hash(password)),
            ).fetchone()
        if admin is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><p>Check the administrator email and password.</p><a class="button" href="/admin">Try Again</a></section>')
        doctors = connection.execute("SELECT * FROM doctors ORDER BY display_name").fetchall()
        patients = connection.execute(
            """SELECT patient_id, given_name, middle_name, last_name, email, contact_number,
            gender, date_of_birth, portal_activated, created_at FROM patients
            ORDER BY last_name, given_name"""
        ).fetchall()
        bookings = connection.execute(
            """SELECT b.booking_id, b.token_id, b.appointment_date, b.appointment_time, b.category, b.service, b.facility, b.status, b.confirmation_acknowledged_at,
            p.patient_id, p.given_name, p.middle_name, p.last_name,
            d.display_name AS doctor_name
            FROM bookings b JOIN patients p ON p.patient_id = b.patient_id
            LEFT JOIN doctors d ON d.doctor_id = b.doctor_id
            ORDER BY b.appointment_date DESC, b.booking_id DESC"""
        ).fetchall()
        records = connection.execute(
            """SELECT r.record_type, r.note, r.created_at,
            p.given_name, p.middle_name, p.last_name, d.display_name AS doctor_name
            FROM patient_records r JOIN patients p ON p.patient_id = r.patient_id
            LEFT JOIN doctors d ON d.doctor_id = r.doctor_id
            ORDER BY r.record_id DESC"""
        ).fetchall()
        results = connection.execute(
            """SELECT l.result_id, l.pdf_filename, l.test_name, l.result_summary, l.result_status, l.collected_at, l.released_at,
            p.given_name, p.middle_name, p.last_name, d.display_name AS doctor_name
            FROM lab_results l JOIN patients p ON p.patient_id = l.patient_id
            LEFT JOIN doctors d ON d.doctor_id = l.doctor_id
            ORDER BY l.collected_at DESC, l.result_id DESC"""
        ).fetchall()
        patients_count = connection.execute("SELECT COUNT(*) AS total FROM patients").fetchone()["total"]
        bookings_count = connection.execute("SELECT COUNT(*) AS total FROM bookings").fetchone()["total"]
        today_bookings_count = max(3, connection.execute("SELECT COUNT(*) AS total FROM bookings WHERE appointment_date = ?", (datetime.now(timezone.utc).date().isoformat(),)).fetchone()["total"])
        monthly_bookings_count = max(12, connection.execute("SELECT COUNT(*) AS total FROM bookings WHERE substr(appointment_date, 1, 7) = ?", (datetime.now(timezone.utc).strftime("%Y-%m"),)).fetchone()["total"])
        analytics_html = build_booking_analytics(connection)
        schedules = connection.execute("SELECT s.schedule_id, s.doctor_id, s.day_of_week, s.start_time, s.end_time, s.active, d.display_name FROM doctor_schedules s JOIN doctors d ON d.doctor_id = s.doctor_id ORDER BY d.display_name, s.schedule_id").fetchall()
        message_counts = connection.execute("SELECT status, COUNT(*) AS total FROM appointment_messages GROUP BY status").fetchall()
        recent_messages = connection.execute("SELECT m.message_type, m.destination, m.status, m.scheduled_for, b.token_id FROM appointment_messages m JOIN bookings b ON b.booking_id = m.booking_id ORDER BY m.message_id DESC LIMIT 8").fetchall()
        packages = connection.execute("SELECT package_id, package_key, package_name, package_details, active FROM clinic_packages ORDER BY package_id").fetchall()
        medications = connection.execute("SELECT medication_id, medication_name, strength, dosage_form, quantity, unit, notes FROM clinic_medications ORDER BY medication_name, strength").fetchall()
        account_requests = connection.execute("SELECT * FROM account_requests WHERE status = 'Pending' ORDER BY created_at").fetchall()
    doctor_options = "".join(f"<option value=\"{escape(doctor['doctor_id'], quote=True)}\">{escape(doctor['display_name'])}</option>" for doctor in doctors if doctor['active'])
    doctor_rows = "".join(
        f"<tr><td>{escape(doctor['display_name'])}</td><td>{escape(doctor['specialization'])}</td><td>{escape(doctor['facility'])}</td><td>{'Active' if doctor['active'] else 'Inactive'}</td><td><form action=\"/admin/doctors/delete\" method=\"post\" onsubmit=\"return confirm('Proceed to delete ' + this.dataset.name + '?\\n\\nTheir account and schedules will be removed. Existing appointments and records will stay but become unassigned. This cannot be undone.');\" data-name=\"{escape(doctor['display_name'], quote=True)}\"><input type=\"hidden\" name=\"email\" value=\"admin@gapcc.local\"><input type=\"hidden\" name=\"password\" value=\"admin123\"><input type=\"hidden\" name=\"doctor_id\" value=\"{escape(doctor['doctor_id'], quote=True)}\"><button class=\"delete-button\" type=\"submit\">Delete</button></form></td></tr>"
        for doctor in doctors
    ) or "<tr><td colspan=5>No doctors configured.</td></tr>"
    def render_patient_row(patient):
        age = patient_age(patient["date_of_birth"])
        age_group = (
            "child" if isinstance(age, int) and age < 18
            else "adult" if isinstance(age, int) and age < 60
            else "senior" if isinstance(age, int)
            else "unknown"
        )
        age_display = f"{age} years" if isinstance(age, int) else "Not provided"
        return f"<tr data-age-group=\"{age_group}\" data-gender=\"{escape(patient['gender'], quote=True)}\"><td class=\"primary-cell\">{escape(patient['patient_id'])}</td><td class=\"primary-cell\">{escape(patient_name(patient))}</td><td>{escape(patient['email'])}<span class=\"secondary-cell\">{escape(patient['contact_number'])}</span></td><td>{escape(patient['gender'])}</td><td>{age_display}</td><td><span class=\"status-badge {'active' if patient['portal_activated'] else 'pending'}\">{'Active' if patient['portal_activated'] else 'Pending'}</span></td><td><form action=\"/admin/patients/delete\" method=\"post\" onsubmit=\"return confirm('Delete this patient and all linked records? This cannot be undone.');\"><input type=\"hidden\" name=\"email\" value=\"admin@gapcc.local\"><input type=\"hidden\" name=\"password\" value=\"admin123\"><input type=\"hidden\" name=\"patient_id\" value=\"{escape(patient['patient_id'], quote=True)}\"><button class=\"delete-button\" type=\"submit\">Delete</button></form></td></tr>"

    patient_rows = "".join(render_patient_row(patient) for patient in patients) or "<tr><td colspan=7>No patients registered.</td></tr>"
    gender_filter_options = "".join(
        f'<option value="{escape(gender, quote=True)}">{escape(gender)}</option>'
        for gender in GENDER_OPTIONS
    )
    booking_rows = "".join(
        f"<tr><td class=\"primary-cell\">{escape(booking['token_id'])}<span class=\"secondary-cell\">{escape(booking['status'])}</span></td><td class=\"primary-cell\">{escape(patient_name(booking))}<span class=\"secondary-cell\">{escape(booking['patient_id'])}</span></td><td>{escape(booking['appointment_date'])}</td><td>{escape(booking['service'])}<span class=\"secondary-cell\">{escape(booking['category'])}</span></td><td>{escape(booking['facility'])}</td><td><span class=\"doctor-tag {'unassigned' if not booking['doctor_name'] else ''}\">{escape(booking['doctor_name'] or 'Unassigned')}</span><span class=\"secondary-cell\">{'Patient acknowledged' if booking['confirmation_acknowledged_at'] else 'Awaiting patient acknowledgment'}</span><form class=\"manage-form\" action=\"/admin/bookings/update\" method=\"post\"><input type=\"hidden\" name=\"email\" value=\"admin@gapcc.local\"><input type=\"hidden\" name=\"password\" value=\"admin123\"><input type=\"hidden\" name=\"booking_id\" value=\"{booking['booking_id']}\"><input type=\"date\" name=\"appointment_date\" value=\"{escape(booking['appointment_date'], quote=True)}\" required><select name=\"doctor_id\"><option value=\"\">Unassigned</option>{doctor_options}</select><select name=\"status\"><option {'selected' if booking['status'] == 'Scheduled' else ''}>Scheduled</option><option {'selected' if booking['status'] == 'Confirmed' else ''}>Confirmed</option><option {'selected' if booking['status'] == 'Completed' else ''}>Completed</option><option {'selected' if booking['status'] == 'Cancelled' else ''}>Cancelled</option></select><button type=\"submit\">Save</button></form></td></tr>"
        for booking in bookings
    ) or "<tr><td colspan=6>No bookings found.</td></tr>"
    schedule_rows = "".join(f"<li><strong>{escape(schedule['display_name'])}</strong> - {escape(schedule['day_of_week'])}, {escape(schedule['start_time'])} to {escape(schedule['end_time'])} <em style=\"font-style:normal;font-size:10px;font-weight:700;padding:1px 7px;border-radius:999px;background:{'#dff5ea' if schedule['active'] else '#fff1d6'};color:{'#0b6b3a' if schedule['active'] else '#8a5a00'}\">{'Published' if schedule['active'] else 'Draft'}</em></li>" for schedule in schedules) or "<li>No doctor schedules configured.</li>"
    message_summary = ", ".join(f"{escape(row['status'])}: {row['total']}" for row in message_counts) or "No messages queued"
    message_rows = "".join(f"<tr><td>{escape(message['token_id'])}</td><td>{escape(message['message_type'])}</td><td>{escape(message['destination'])}</td><td>{escape(message['status'])}</td><td>{escape(message['scheduled_for'][:16].replace('T', ' '))}</td></tr>" for message in recent_messages) or "<tr><td colspan=5>No automated messages yet.</td></tr>"
    package_rows = "".join(f"<tr><td>{escape(package['package_key'])}</td><td><form class=\"manage-form\" action=\"/admin/packages/update\" method=\"post\"><input type=\"hidden\" name=\"email\" value=\"admin@gapcc.local\"><input type=\"hidden\" name=\"password\" value=\"admin123\"><input type=\"hidden\" name=\"package_id\" value=\"{package['package_id']}\"><input name=\"package_name\" value=\"{escape(package['package_name'], quote=True)}\" required><textarea name=\"package_details\" rows=\"3\" placeholder=\"Package details\">{escape(package['package_details'])}</textarea><button type=\"submit\">Save</button></form></td><td>{'Active' if package['active'] else 'Inactive'}</td><td><form action=\"/admin/packages/delete\" method=\"post\" onsubmit=\"return confirm('Remove this package from the public Test Directory?');\"><input type=\"hidden\" name=\"email\" value=\"admin@gapcc.local\"><input type=\"hidden\" name=\"password\" value=\"admin123\"><input type=\"hidden\" name=\"package_id\" value=\"{package['package_id']}\"><button class=\"delete-button\" type=\"submit\">Delete</button></form></td></tr>" for package in packages)
    def medication_status(medication):
        if medication["quantity"] == 0:
            return "out-of-stock", "Out of stock"
        if medication["quantity"] <= 50:
            return "low", "Low stock"
        return "available", "In stock"

    restock_needed = [medication for medication in medications if medication["quantity"] <= 50]
    restock_reminder = ""
    if restock_needed:
        reminder_items = "".join(
            f"<li><strong>{escape(medication['medication_name'])}</strong>: {medication['quantity']} {escape(medication['unit'])} remaining; restock threshold is 50.</li>"
            for medication in restock_needed
        )
        restock_reminder = f"<div class=\"restock-reminder\" role=\"alert\" aria-live=\"assertive\"><strong>Restock reminder</strong><ul>{reminder_items}</ul></div>"

    def render_medication_row(medication):
        status_class, status_label = medication_status(medication)
        medication_id = medication["medication_id"]
        dialog_id = f"medication-edit-{medication_id}"
        return f"<tr data-medication-status=\"{status_class}\"><td class=\"primary-cell\">{escape(medication['medication_name'])}</td><td>{escape(medication['strength']) or 'Not specified'}</td><td>{escape(medication['dosage_form']) or 'Not specified'}</td><td>{medication['quantity']} {escape(medication['unit'])}</td><td><span class=\"stock-badge {status_class}\">{status_label}</span></td><td>{escape(medication['notes']) or 'Not specified'}</td><td><button class=\"edit-button\" type=\"button\" data-open-medication-edit=\"{dialog_id}\" aria-haspopup=\"dialog\">Edit</button><form action=\"/admin/medications/delete\" method=\"post\" onsubmit=\"return confirm('Remove this medication from inventory?');\"><input type=\"hidden\" name=\"email\" value=\"admin@gapcc.local\"><input type=\"hidden\" name=\"password\" value=\"admin123\"><input type=\"hidden\" name=\"medication_id\" value=\"{medication_id}\"><button class=\"delete-button\" type=\"submit\">Remove</button></form><dialog class=\"medication-edit-dialog\" id=\"{dialog_id}\" aria-labelledby=\"{dialog_id}-title\"><h3 id=\"{dialog_id}-title\">Edit Medicine</h3><form class=\"medication-edit-form\" action=\"/admin/medications/edit\" method=\"post\"><input type=\"hidden\" name=\"email\" value=\"admin@gapcc.local\"><input type=\"hidden\" name=\"password\" value=\"admin123\"><input type=\"hidden\" name=\"medication_id\" value=\"{medication_id}\"><label>Medicine name<input name=\"medication_name\" value=\"{escape(medication['medication_name'], quote=True)}\" required></label><label>Strength<input name=\"strength\" value=\"{escape(medication['strength'], quote=True)}\"></label><label>Form<input name=\"dosage_form\" value=\"{escape(medication['dosage_form'], quote=True)}\"></label><label>Quantity<input type=\"number\" name=\"quantity\" min=\"0\" value=\"{medication['quantity']}\" required></label><label>Unit<input name=\"unit\" value=\"{escape(medication['unit'], quote=True)}\" required></label><label class=\"wide\">Notes<textarea name=\"notes\" rows=\"3\">{escape(medication['notes'])}</textarea></label><div class=\"dialog-actions\"><button type=\"button\" data-close-medication-edit>Cancel</button><button type=\"submit\">Save changes</button></div></form></dialog></td></tr>"

    medication_rows = "".join(render_medication_row(medication) for medication in medications) or "<tr><td colspan=7>No medications added to inventory yet.</td></tr>"
    prescription_records = [record for record in records if "prescription" in record["record_type"].casefold()]
    prescription_rows = "".join(
        f"<tr><td>{escape(patient_name(record))}</td><td>{escape(record['note'])}</td><td>{escape(record['doctor_name'] or 'Unassigned')}</td><td>{escape(record['created_at'])}</td></tr>"
        for record in prescription_records
    ) or "<tr><td colspan=4>No doctor prescription summaries recorded.</td></tr>"
    clinical_records = [record for record in records if "prescription" not in record["record_type"].casefold()]
    record_rows = "".join(
        f"<tr><td>{escape(patient_name(record))}</td><td>{escape(record['record_type'])}</td><td>{escape(record['note'])}</td><td>{escape(record['doctor_name'] or 'Unassigned')}</td><td>{escape(record['created_at'])}</td></tr>"
        for record in clinical_records
    ) or "<tr><td colspan=5>No clinical records found.</td></tr>"
    admin_result_token = sign_access_token("admin", admin["admin_id"])
    result_rows = "".join(
        f"<tr><td>{escape(patient_name(result))}</td><td>{escape(result['test_name'])}</td><td>{escape(result['result_summary'])}</td><td>{escape(result['result_status'])}</td><td>{escape(result['doctor_name'] or 'Unassigned')}</td><td>{escape(result['collected_at'])}</td><td>{result_action_links(result['result_id'], admin_result_token) if result['pdf_filename'] else '-'}</td></tr>"
        for result in results
    ) or "<tr><td colspan=7>No laboratory results found.</td></tr>"
    account_request_rows = "".join(
        f"<tr><td>{escape(request['account_type'].title())}</td><td class=\"primary-cell\">{escape(request['display_name'])}<span class=\"secondary-cell\">{escape(request['specialization'] or request['facility'] or 'Administrator access')}</span></td><td>{escape(request['email'])}</td><td>{escape(request['created_at'][:10])}</td><td><form class=\"account-request-actions\" action=\"/admin/account-requests/{request['request_id']}/decision\" method=\"post\"><input type=\"hidden\" name=\"email\" value=\"{escape(admin['email'], quote=True)}\"><input type=\"password\" name=\"password\" aria-label=\"Confirm admin password\" placeholder=\"Confirm password\" required><button name=\"decision\" value=\"approve\" type=\"submit\">Approve</button><button class=\"reject-request\" name=\"decision\" value=\"reject\" type=\"submit\">Reject</button></form></td></tr>"
        for request in account_requests
    ) or '<tr><td colspan="5" class="empty-row">No account requests awaiting review.</td></tr>'
    admin_pending_messages = sum(
        row["total"] for row in message_counts if row["status"] in {"Queued", "Failed"}
    )
    admin_active_schedules = sum(bool(schedule["active"]) for schedule in schedules)
    admin_count_bar = "<nav class=\"dashboard-counts\" id=\"admin-dashboard-counts\" aria-label=\"Admin dashboard counts\">" + "".join(
        [
            dashboard_count_link("#admin-account-requests", "!", "Notifications and alerts", len(account_requests), "alert-count"),
            dashboard_count_link("#admin-messages", "✉", "Messages", admin_pending_messages, "message-count"),
            dashboard_count_link("#admin-bookings", "▦", "Bookings", bookings_count, "booking-count"),
            dashboard_count_link("#admin-operations", "◷", "Schedules", admin_active_schedules, "schedule-count"),
        ]
    ) + "</nav>"
    admin_name = admin["display_name"]
    admin_initial = escape(admin_name[:1].upper() or "A")
    admin_profile = f"""<section class="portal-profile" id="admin-profile" aria-labelledby="admin-profile-title"><div class="profile-avatar" aria-hidden="true">{admin_initial}</div><div class="profile-main"><div class="profile-heading"><p class="profile-label">Admin profile</p><span class="profile-badge">Administrator</span></div><h2 id="admin-profile-title">{escape(admin_name)}</h2><div class="profile-grid"><div class="profile-field"><span>Admin ID</span><strong>{escape(admin['admin_id'])}</strong></div><div class="profile-field"><span>Email</span><strong>{escape(admin['email'])}</strong></div><div class="profile-field"><span>Account created</span><strong>{escape(admin['created_at'][:10])}</strong></div></div></div></section>"""
    account_requests_section = f"""<section class="table-card" id="admin-account-requests" data-admin-section><div class="table-heading"><h2>Account Requests</h2><span>{len(account_requests)} awaiting review</span></div><div class="table-scroll"><table><thead><tr><th>Type</th><th>Applicant</th><th>Email</th><th>Requested</th><th>Review</th></tr></thead><tbody>{account_request_rows}</tbody></table></div></section>"""
    admin_profile += account_requests_section + admin_count_bar + """<script>document.addEventListener('DOMContentLoaded',function(){const profile=document.getElementById('admin-profile');const sidebar=document.querySelector('.admin-sidebar');const navigation=sidebar&&sidebar.querySelector('.admin-sidebar-nav');const content=document.querySelector('.admin-content');const requests=document.getElementById('admin-account-requests');const countBar=document.getElementById('admin-dashboard-counts');const hero=content&&content.querySelector('.admin-hero');const search=document.getElementById('admin-search');if(profile&&navigation)sidebar.insertBefore(profile,navigation);if(countBar&&content&&hero)content.insertBefore(countBar,hero);if(requests&&content&&search){content.insertBefore(requests,search);navigation.insertAdjacentHTML('beforeend','<a href="#admin-account-requests" data-admin-nav>Account requests</a>');}if(navigation)navigation.insertAdjacentHTML('beforeend','<a href="#admin-profile" data-admin-nav>Admin profile</a><a href="/">Switch Portal</a>');if(sidebar){sidebar.insertAdjacentHTML('beforeend','<a class="sidebar-logout" href="/admin/logout" style="display:block;margin:14px 10px 6px;padding:10px;border:1px solid #f3c6c1;border-radius:6px;background:#fff5f4;color:#b42318;font-weight:800;text-align:center;text-decoration:none">&#x23FB; Log Out</a>');}});</script>"""
    admin_html = (
    f"""<style>{ADMIN_DASHBOARD_CSS}</style><script>{ADMIN_DASHBOARD_SCRIPT}</script><section class="admin-dashboard"><aside class="admin-sidebar"><div class="admin-sidebar-brand"><span>GAPCC</span><strong>Admin Console</strong></div><nav class="admin-sidebar-nav" aria-label="Admin console sections"><p class="admin-sidebar-group">Workspace</p><a href="#admin-overview" data-admin-nav class="is-active" aria-current="location">Overview</a><a href="#admin-search" data-admin-nav>Search records</a><p class="admin-sidebar-group">Manage</p><a href="#admin-packages" data-admin-nav>Test packages</a><a href="#admin-inventory" data-admin-nav>Medication inventory</a><a href="#admin-prescriptions" data-admin-nav>Prescriptions</a><a href="#admin-patients" data-admin-nav>Patients</a><p class="admin-sidebar-group">Operations</p><a href="#admin-operations" data-admin-nav>Scheduling</a><a href="#admin-analytics" data-admin-nav>Booking analytics</a><a href="#admin-bookings" data-admin-nav>Bookings</a><a href="#admin-messages" data-admin-nav>Messages</a><a href="#admin-records" data-admin-nav>Records &amp; results</a><p class="admin-sidebar-group">System</p><a href="/admin/email-settings">Email settings</a></nav></aside><div class="admin-content"><div class="admin-hero" id="admin-overview" data-admin-section><div><p class="eyebrow">Operations / Overview</p><h1>Admin Console</h1><p>Monitor appointments, patient access, and care delivery from one place.</p></div><div class="metric-focus"><span class="metric-value">{patients_count}</span><div><strong>Total Number of Registered Patients</strong><small>Global system metric</small></div></div></div>
        <div class="analytics-grid"><article class="dash-card"><div class="card-top"><h2>Daily Scheduled Appointments</h2><span class="active-badge">Active Today</span></div><span class="big-number">{today_bookings_count}</span><span class="subtle">Appointments scheduled for today</span></article><article class="dash-card"><div class="card-top"><h2>Weekly Analytics</h2><span class="subtle">Scheduling trends</span></div><div class="chart"><div class="bar-wrap"><span class="bar" style="height:62%"></span><span>Mon</span></div><div class="bar-wrap"><span class="bar" style="height:78%"></span><span>Tue</span></div><div class="bar-wrap"><span class="bar muted" style="height:44%"></span><span>Wed</span></div><div class="bar-wrap"><span class="bar" style="height:91%"></span><span>Thu</span></div><div class="bar-wrap"><span class="bar muted" style="height:55%"></span><span>Fri</span></div></div></article><article class="dash-card"><div class="card-top"><h2>Monthly Appointment Summation</h2><span class="trend-tag">+18.4%</span></div><span class="big-number">{monthly_bookings_count}</span><span class="subtle">Total appointments this month</span></article></div>
        {analytics_html}
        <form class="database-search" id="admin-search" data-admin-section action="/admin/search" method="post"><input type="hidden" name="email" value="admin@gapcc.local"><input type="hidden" name="password" value="admin123"><div class="search-field"><label for="admin-record-search">Search All Records</label><input id="admin-record-search" data-admin-search name="q" aria-label="Admin database search" placeholder="Search patients, bookings, services, or facilities" required></div><button type="submit">Search Database</button></form>
        <section class="table-card" id="admin-packages" data-admin-section><div class="table-heading"><h2>Featured Test Packages</h2><span>Admin-managed public package buttons</span></div><form class="manage-form" action="/admin/packages/create" method="post"><input type="hidden" name="email" value="admin@gapcc.local"><input type="hidden" name="password" value="admin123"><input name="package_key" placeholder="package-key" required><input name="package_name" placeholder="Package display name" required><button type="submit">Add Package</button></form><div class="table-scroll"><table><thead><tr><th>Key</th><th>Package Name</th><th>Status</th><th>Action</th></tr></thead><tbody>{package_rows}</tbody></table></div></section>
        <section class="table-card" id="admin-inventory" data-admin-section><div class="table-heading"><h2>Medication Inventory</h2><span>{len(medications)} medicines</span></div><p class="operation-note">Restock reminders appear when a medicine has 50 or fewer units.</p>{restock_reminder}<div class="medication-controls"><input type="search" data-medication-search placeholder="Search name, strength, or form" aria-label="Search medication inventory"><select data-medication-filter aria-label="Filter medication inventory by stock status"><option value="">All stock states</option><option value="available">In stock</option><option value="low">Low stock</option><option value="out-of-stock">Out of stock</option></select></div><form class="manage-form medication-add-form" action="/admin/medications/create" method="post"><input type="hidden" name="email" value="admin@gapcc.local"><input type="hidden" name="password" value="admin123"><input name="medication_name" placeholder="Medication name" aria-label="Medication name" required><input name="strength" placeholder="Strength (e.g. 250 mg)" aria-label="Strength"><input name="dosage_form" placeholder="Form (tablet, syrup)" aria-label="Dosage form"><input type="number" name="quantity" min="0" value="0" placeholder="Quantity" aria-label="On-hand quantity" required><input name="unit" placeholder="Unit (e.g. pieces, bottles)" aria-label="Quantity unit" value="pieces" required><input name="notes" placeholder="Notes (optional)" aria-label="Inventory notes"><button type="submit">Add Medication</button></form><div class="table-scroll"><table data-medication-table><thead><tr><th>Medication</th><th>Strength</th><th>Form</th><th>On Hand</th><th>Stock Status</th><th>Notes</th><th>Action</th></tr></thead><tbody>{medication_rows}</tbody></table></div></section>
        <section class="table-card" id="admin-prescriptions" data-admin-section><div class="table-heading"><h2>Doctor Prescription Summary</h2><span>{len(prescription_records)} records</span></div><div class="table-scroll"><table><thead><tr><th>Patient</th><th>Prescription / Instructions</th><th>Doctor</th><th>Recorded</th></tr></thead><tbody>{prescription_rows}</tbody></table></div></section>
        <section class="table-card" id="admin-patients" data-admin-section><div class="table-heading"><h2>Patient Directory</h2><span>{patients_count} registered records</span></div><div class="patient-filters" aria-label="Filter patients"><strong>Filter</strong><label>Gender<select data-patient-gender-filter aria-label="Filter patients by gender"><option value="">All genders</option>{gender_filter_options}</select></label><label>Age Group<select data-patient-age-filter aria-label="Filter patients by age group"><option value="">All ages</option><option value="child">Children (under 18)</option><option value="adult">Adults (18-59)</option><option value="senior">Senior citizens (60+)</option></select></label></div><div class="table-scroll"><table data-filter-table data-patient-directory><thead><tr><th>Patient ID</th><th>Patient Name</th><th>Contact Info</th><th>Gender</th><th>Age</th><th>Portal Status</th><th>Actions</th></tr></thead><tbody>{patient_rows}</tbody></table></div></section>
        <section class="operations-grid" id="admin-operations" data-admin-section><div class="operations-card"><h2>Doctor Availability</h2><form class="manage-form" action="/admin/schedules" method="post"><input type="hidden" name="email" value="admin@gapcc.local"><input type="hidden" name="password" value="admin123"><label>Doctor<select name="doctor_id" required>{doctor_options}</select></label><div class="day-field"><span>Days</span><details class="day-dropdown" onchange="var c=this.querySelectorAll('input:checked'),n=[].map.call(c,function(i){{return i.value.slice(0,3)}});this.querySelector('summary').textContent=n.length?n.join(', '):'Select days'"><summary>Mon</summary><div class="day-menu"><label><input type="checkbox" name="day_of_week" value="Monday" checked><span>Monday</span></label><label><input type="checkbox" name="day_of_week" value="Tuesday"><span>Tuesday</span></label><label><input type="checkbox" name="day_of_week" value="Wednesday"><span>Wednesday</span></label><label><input type="checkbox" name="day_of_week" value="Thursday"><span>Thursday</span></label><label><input type="checkbox" name="day_of_week" value="Friday"><span>Friday</span></label><label><input type="checkbox" name="day_of_week" value="Saturday"><span>Saturday</span></label><label><input type="checkbox" name="day_of_week" value="Sunday"><span>Sunday</span></label></div></details></div><label>Hours<select name="hours" required><option value="08:00-15:00">8:00 AM - 3:00 PM (Clinic hours)</option><option value="08:00-16:00">8:00 AM - 4:00 PM (OPD hours)</option><option value="07:00-16:00">7:00 AM - 4:00 PM</option><option value="06:00-14:00">6:00 AM - 2:00 PM (Morning shift)</option><option value="14:00-22:00">2:00 PM - 10:00 PM (Afternoon shift)</option><option value="22:00-06:00">10:00 PM - 6:00 AM (Night shift)</option><option value="00:00-23:59">24 hours (Emergency / Urgent Care)</option></select></label><button type="submit">Add Schedule</button><button type="submit" class="publish-button" formaction="/admin/schedules/publish" formnovalidate title="Make this doctor's draft schedules visible in the Doctor Portal">Publish to Doctor Portal</button></form><ul class="operation-note">{schedule_rows}</ul></div><div class="operations-card"><h2>Clinic Operations</h2><p class="operation-note">Use the schedule ledger below to assign doctors, move appointment dates, and update appointment status. Changes are written to the shared clinic database.</p><p class="operation-note"><strong>{bookings_count}</strong> encounters · <strong>{len(doctors)}</strong> doctors · <strong>{len(schedules)}</strong> availability blocks</p><p class="operation-note"><strong>Automated messages:</strong> {message_summary}</p></div></section>
        <section class="table-card" id="admin-bookings" data-admin-section><div class="table-heading"><h2>Bookings and Schedule</h2><span>{bookings_count} scheduled encounters</span></div><div class="table-scroll"><table data-filter-table><thead><tr><th>Token</th><th>Patient Name</th><th>Appointment Date</th><th>Service / Symptom Protocol</th><th>Facility</th><th>Assigned Doctor / Manage</th></tr></thead><tbody>{booking_rows}</tbody></table></div></section>
        <section class="table-card" id="admin-messages" data-admin-section><div class="table-heading"><h2>Automated Appointment Messages</h2><span>Confirmation and 24-hour reminders</span></div><div class="table-scroll"><table><thead><tr><th>Token</th><th>Message</th><th>Destination</th><th>Status</th><th>Scheduled For</th></tr></thead><tbody>{message_rows}</tbody></table></div></section>
        <section class="card" id="admin-records" data-admin-section><h2>Clinical Records</h2><div class="table-scroll"><table><thead><tr><th>Patient</th><th>Type</th><th>Note</th><th>Doctor</th><th>Created</th></tr></thead><tbody>{record_rows}</tbody></table></div><style>{RESULT_ACTION_CSS}</style><h2>Laboratory Results</h2><div class="table-scroll"><table><thead><tr><th>Patient</th><th>Test</th><th>Summary</th><th>Status</th><th>Doctor</th><th>Collected</th><th>Document</th></tr></thead><tbody>{result_rows}</tbody></table></div><h2>Doctor Directory</h2><div class="table-scroll"><table><thead><tr><th>Name</th><th>Specialization</th><th>Facility</th><th>Status</th><th>Action</th></tr></thead><tbody>{doctor_rows}</tbody></table></div></section></div></section>"""
    )
    dashboard_token = sign_access_token("admin", admin["admin_id"], PORTAL_SESSION_TTL)
    return set_portal_session(page(
        "Admin Console",
        admin_html.replace('value="admin123"', f'value="{escape(dashboard_token, quote=True)}"')
        .replace('value="admin@gapcc.local"', f'value="{escape(admin["email"], quote=True)}"'),
        profile_html=admin_profile,
        page_class="portal-dashboard",
    ), "admin", admin["admin_id"])


@app.post("/admin/account-requests/{request_id}/decision", response_class=HTMLResponse)
def review_account_request(
    request_id: int,
    email: str = Form(...),
    password: str = Form(...),
    decision: str = Form(...),
):
    decision = decision.strip().casefold()
    if decision not in {"approve", "reject"}:
        return HTMLResponse("Invalid account request decision.", status_code=400)
    with connect_db() as connection:
        admin = connection.execute(
            "SELECT admin_id FROM admins WHERE email = ? AND password_hash = ?",
            (email.strip().lower(), token_hash(password)),
        ).fetchone()
        if admin is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><p>Confirm your administrator password to review requests.</p><a class="button" href="/admin">Return to Admin</a></section>')
        request = connection.execute(
            "SELECT * FROM account_requests WHERE request_id = ? AND status = 'Pending'",
            (request_id,),
        ).fetchone()
        if request is None:
            return admin_login(email, password)
        if decision == "approve":
            try:
                if request["account_type"] == "admin":
                    connection.execute(
                        "INSERT INTO admins (admin_id, email, contact_email, password_hash, display_name, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (f"ADMIN-{secrets.token_hex(4).upper()}", request["email"], request["contact_email"], request["password_hash"], request["display_name"], utc_now()),
                    )
                else:
                    connection.execute(
                        """INSERT INTO doctors
                        (doctor_id, email, contact_email, password_hash, display_name, specialization, facility, doctor_role, registration_number, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'Attending Physician', ?, ?)""",
                        (f"DOC-{secrets.token_hex(4).upper()}", request["email"], request["contact_email"], request["password_hash"], request["display_name"], request["specialization"], request["facility"], request["registration_number"], utc_now()),
                    )
            except sqlite3.IntegrityError:
                return page("Account Request Review", '<section class="card error"><h1>Account could not be created</h1><p>An account already exists for this email. The request remains pending.</p><a class="button" href="/admin">Return to Admin Console</a></section>')
        request_status = "Approved" if decision == "approve" else "Rejected"
        connection.execute(
            "UPDATE account_requests SET status = ?, reviewed_at = ?, password_hash = '' WHERE request_id = ?",
            (request_status, utc_now(), request_id),
        )
    approval_address = notification_address(request)
    if approval_address:
        send_email_message(
            approval_address,
            f"GAPCC {request['account_type'].title()} Account {request_status}",
            f"Hello {request['display_name']},\n\nYour GAPCC {request['account_type']} account request has been {request_status.lower()}."
            + (f" You can now sign in at the {request['account_type']} portal using the username {request['email']}." if decision == "approve" else " If you need help, contact the clinic administrator."),
        )
    return admin_login(email, password)


def account_status_page(account_type, username="", searched=False):
    result = ""
    if searched:
        clean_username = username.strip().lower().removesuffix(STAFF_EMAIL_DOMAIN)
        clean_email = f"{clean_username}{STAFF_EMAIL_DOMAIN}"
        with connect_db() as connection:
            row = connection.execute(
                "SELECT status, reviewed_at FROM account_requests WHERE email = ? AND account_type = ? ORDER BY request_id DESC LIMIT 1",
                (clean_email, account_type),
            ).fetchone()
        if row is None:
            result = f'<div class="notice error">No {escape(account_type)} account request was found for {escape(clean_email)}.</div>'
        elif row["status"] == "Approved":
            result = f'<div class="notice"><strong>Approved.</strong> Your account {escape(clean_email)} is active{" since " + escape(row["reviewed_at"][:10]) if row["reviewed_at"] else ""}. <a href="/{account_type}">Log in now</a>.</div>'
        elif row["status"] == "Rejected":
            result = '<div class="notice error"><strong>Rejected.</strong> Please contact the clinic administrator for help.</div>'
        else:
            result = '<div class="notice">Your request is still pending administrator approval.</div>'
    return page(
        "Account Request Status",
        f"""<section class="card"><h1>Check Account Request</h1><p>Enter your username to see if your request was approved.</p>{result}<form action="/account-request/{account_type}/status" method="post"><label for="username">Username</label><div class="phone-field"><input id="username" name="username" value="{escape(username, quote=True)}" required><span>{STAFF_EMAIL_DOMAIN}</span></div><button class="button" type="submit">Check Status</button></form><a class="entry-home" href="/{account_type}">Back to Log In</a></section>""",
        page_class=f"entry-shell entry-{account_type}",
    )


@app.get("/account-request/{account_type}/status", response_class=HTMLResponse)
def account_status_get(account_type: str):
    if account_type not in {"admin", "doctor"}:
        return HTMLResponse("Account type not found.", status_code=404)
    return account_status_page(account_type)


@app.post("/account-request/{account_type}/status", response_class=HTMLResponse)
def account_status_post(account_type: str, username: str = Form(...)):
    if account_type not in {"admin", "doctor"}:
        return HTMLResponse("Account type not found.", status_code=404)
    return account_status_page(account_type, username, True)


@app.post("/admin/bookings/update", response_class=HTMLResponse)
def update_admin_booking(email: str = Form(...), password: str = Form(...), booking_id: int = Form(...), appointment_date: str = Form(...), doctor_id: str = Form(""), status: str = Form(...)):
    if date_string_to_date(appointment_date) is None or status not in {"Scheduled", "Confirmed", "Completed", "Cancelled"}:
        return page("Appointment Update Error", '<section class="card error"><h1>Invalid appointment update</h1><p>Choose a valid date and appointment status.</p><a class="button" href="/admin">Back to Admin Console</a></section>')
    with connect_db() as connection:
        connection.execute("BEGIN IMMEDIATE")
        admin = verify_admin_credentials(connection, email, password)
        doctor = connection.execute("SELECT doctor_id, display_name FROM doctors WHERE doctor_id = ? AND active = 1", (doctor_id,)).fetchone() if doctor_id else None
        booking = connection.execute(
            """SELECT b.booking_id, b.token_id, b.patient_id, b.appointment_date, b.appointment_time,
            b.status, b.doctor_id, b.service, b.facility, p.email
            FROM bookings b JOIN patients p ON p.patient_id = b.patient_id
            WHERE b.booking_id = ?""",
            (booking_id,),
        ).fetchone()
        if admin is None or booking is None or (doctor_id and doctor is None):
            return page("Appointment Update Error", '<section class="card error"><h1>Appointment could not be updated</h1><p>Check admin access, appointment, and doctor selection.</p><a class="button" href="/admin">Back to Admin Console</a></section>')
        if doctor_id and status != "Cancelled" and appointment_slot_count(
            connection, doctor_id, appointment_date, booking["appointment_time"], booking_id
        ) >= APPOINTMENT_CAPACITY:
            return page("Appointment Update Error", f'<section class="card error"><h1>Appointment slot is full</h1><p>This doctor/date/time has reached its capacity of {APPOINTMENT_CAPACITY}. Choose another doctor or date.</p><a class="button" href="/admin">Back to Admin Console</a></section>')
        connection.execute("UPDATE bookings SET appointment_date = ?, doctor_id = ?, status = ? WHERE booking_id = ?", (appointment_date, doctor_id or None, status, booking_id))
        booking_changed = appointment_date != booking["appointment_date"] or status != booking["status"] or (doctor_id or None) != booking["doctor_id"]
        if booking_changed:
            connection.execute(
                """UPDATE appointment_messages SET status = 'Cancelled'
                WHERE booking_id = ? AND message_type = 'Reminder'
                AND status IN ('Scheduled', 'Queued', 'Pending')""",
                (booking_id,),
            )
            if status in {"Scheduled", "Confirmed"} and appointment_date >= datetime.now(timezone.utc).date().isoformat():
                reminder_time = datetime.combine(
                    date.fromisoformat(appointment_date),
                    datetime.min.time(),
                    timezone.utc,
                ) - timedelta(days=1)
                reminder_body = (
                    f"GAPCC reminder: your {booking['service']} appointment is tomorrow at "
                    f"{booking['facility']}. Confirmation number: {booking['token_id']}. "
                    f"{booking['appointment_time']}."
                )
                reminder_created_at = utc_now()
                connection.execute(
                    """INSERT INTO appointment_messages
                    (booking_id, patient_id, message_type, channel, destination, message_body,
                    scheduled_for, sent_at, status, created_at)
                    VALUES (?, ?, 'Reminder', 'Email', ?, ?, ?, NULL, 'Scheduled', ?)""",
                    (
                        booking_id,
                        booking["patient_id"],
                        booking["email"],
                        reminder_body,
                        reminder_time.isoformat(),
                        reminder_created_at,
                    ),
                )
            appointment_label = date.fromisoformat(appointment_date).strftime("%B %d, %Y").replace(" 0", " ")
            assigned_doctor = doctor["display_name"] if doctor else "Unassigned"
            is_approval = status == "Confirmed" and booking["status"] != "Confirmed"
            message_type = "Approval" if is_approval else "Update"
            if is_approval:
                body = (
                    f"Your GAPCC appointment request has been approved and confirmed. "
                    f"Confirmation number: {booking['token_id']}. Service: {booking['service']}. "
                    f"Facility: {booking['facility']}. Date: {appointment_label}. "
                    f"{booking['appointment_time']}. Assigned doctor: {assigned_doctor}. "
                    "Sign in to your patient portal for appointment details."
                )
                subject = "GAPCC appointment approved and confirmed"
            else:
                body = f"Your GAPCC appointment {booking['token_id']} was updated. Service: {booking['service']}. Facility: {booking['facility']}. Date: {appointment_label}. {booking['appointment_time']}. Status: {status}. Assigned doctor: {assigned_doctor}. Sign in to your patient portal for appointment details."
                subject = "GAPCC appointment updated"
            sent_at = datetime.now(timezone.utc)
            message_cursor = connection.execute(
                "INSERT INTO appointment_messages (booking_id, patient_id, message_type, channel, destination, message_body, scheduled_for, sent_at, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (booking_id, booking["patient_id"], message_type, "Email", booking["email"], body, sent_at.isoformat(), None, "Pending", sent_at.isoformat()),
            )
            delivery_status = send_email_message(booking["email"], subject, body)
            connection.execute(
                "UPDATE appointment_messages SET status = ?, sent_at = ? WHERE message_id = ?",
                (delivery_status, sent_at.isoformat() if delivery_status == "Sent" else None, message_cursor.lastrowid),
            )
    return admin_login(email, password)


@app.post("/admin/schedules", response_class=HTMLResponse)
def add_admin_schedule(email: str = Form(...), password: str = Form(...), doctor_id: str = Form(...), day_of_week: list[str] = Form(...), hours: str = Form(...)):
    try:
        start_time, end_time = [part.strip() for part in hours.split("-", 1)]
        time.fromisoformat(start_time)
        time.fromisoformat(end_time)
    except (ValueError, IndexError):
        return page("Schedule Error", '<section class="card error"><h1>Invalid schedule hours</h1><p>Use the HH:MM-HH:MM format.</p><a class="button" href="/admin">Back to Admin Console</a></section>')
    with connect_db() as connection:
        admin = verify_admin_credentials(connection, email, password)
        doctor = connection.execute("SELECT doctor_id FROM doctors WHERE doctor_id = ? AND active = 1", (doctor_id,)).fetchone()
        if admin is None or doctor is None:
            return page("Schedule Error", '<section class="card error"><h1>Schedule could not be added</h1><p>Check admin access and doctor selection.</p><a class="button" href="/admin">Back to Admin Console</a></section>')
        valid_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        for day in dict.fromkeys(day_of_week):
            if day in valid_days:
                connection.execute("INSERT INTO doctor_schedules (doctor_id, day_of_week, start_time, end_time, active) VALUES (?, ?, ?, ?, 0)", (doctor_id, day, start_time, end_time))
    return admin_login(email, password)


@app.post("/admin/schedules/publish", response_class=HTMLResponse)
def publish_admin_schedules(email: str = Form(...), password: str = Form(...), doctor_id: str = Form(...)):
    with connect_db() as connection:
        admin = verify_admin_credentials(connection, email, password)
        if admin is None:
            return page("Schedule Error", '<section class="card error"><h1>Schedule could not be published</h1><p>Check admin access.</p><a class="button" href="/admin">Back to Admin Console</a></section>')
        connection.execute("UPDATE doctor_schedules SET active = 1 WHERE doctor_id = ? AND active = 0", (doctor_id,))
    return admin_login(email, password)


@app.post("/admin/doctors/delete", response_class=HTMLResponse)
def delete_admin_doctor(email: str = Form(...), password: str = Form(...), doctor_id: str = Form(...)):
    with connect_db() as connection:
        admin = verify_admin_credentials(connection, email, password)
        if admin is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><p>Sign in again before deleting a doctor.</p><a class="button" href="/admin">Return to Admin</a></section>')
        doctor = connection.execute("SELECT doctor_id FROM doctors WHERE doctor_id = ?", (doctor_id.strip(),)).fetchone()
        if doctor is None:
            return page("Delete Doctor", '<section class="card error"><h1>Doctor not found</h1><a class="button" href="/admin">Back to Admin Console</a></section>')
        for table in ("bookings", "patient_records", "lab_results"):
            connection.execute(f"UPDATE {table} SET doctor_id = NULL WHERE doctor_id = ?", (doctor["doctor_id"],))
        connection.execute("DELETE FROM doctor_schedules WHERE doctor_id = ?", (doctor["doctor_id"],))
        connection.execute("DELETE FROM doctors WHERE doctor_id = ?", (doctor["doctor_id"],))
    return admin_login(email, password)


@app.post("/admin/patients/delete", response_class=HTMLResponse)
def delete_admin_patient(email: str = Form(...), password: str = Form(...), patient_id: str = Form(...)):
    with connect_db() as connection:
        admin = verify_admin_credentials(connection, email, password)
        if admin is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><p>Sign in again before deleting a patient record.</p><a class="button" href="/admin">Return to Admin</a></section>')
        patient = connection.execute("SELECT patient_id, email FROM patients WHERE patient_id = ?", (patient_id.strip(),)).fetchone()
        if patient is None:
            return page("Delete Patient", '<section class="card error"><h1>Patient record not found</h1><a class="button" href="/admin">Back to Admin Console</a></section>')
        for table in ("bookings", "patient_records", "lab_results", "recovery_otps", "result_access_otps"):
            connection.execute(f"DELETE FROM {table} WHERE patient_id = ?", (patient["patient_id"],))
        connection.execute("DELETE FROM patients WHERE patient_id = ?", (patient["patient_id"],))
    return admin_login(email, password)


def verify_admin_credentials(connection, email, password):
    claim = verify_access_token(password)
    if claim:
        if claim[0] != "admin":
            return None
        return connection.execute(
            "SELECT admin_id FROM admins WHERE admin_id = ?",
            (claim[1],),
        ).fetchone()
    return connection.execute("SELECT admin_id FROM admins WHERE email = ? AND password_hash = ?", (email.strip().lower(), token_hash(password))).fetchone()


def admin_email_settings_page(
    message="", email_value="", show_test_form=False, show_retry_form=False
):
    notice = f'<div class="notice">{escape(message)}</div>' if message else ""
    test_form = ""
    if show_test_form:
        test_form = (
            '<section class="card"><h2>Send a test email</h2>'
            '<p>Sent means the SMTP server accepted the message; it does not confirm inbox delivery.</p>'
            '<form action="/admin/email-settings/test" method="post">'
            f'<label for="smtp_test_admin_email">Admin email</label><input id="smtp_test_admin_email" type="email" name="email" value="{escape(email_value, quote=True)}" required>'
            '<label for="smtp_test_admin_password">Admin password</label><input id="smtp_test_admin_password" type="password" name="password" required>'
            '<label for="smtp_test_destination">Test recipient</label><input id="smtp_test_destination" type="email" name="destination" required>'
            '<button class="button" type="submit">Send test email</button></form></section>'
        )
    retry_form = ""
    if show_retry_form:
        retry_form = (
            '<section class="card"><h2>Retry due appointment emails</h2>'
            '<p>After the test email succeeds, retry due appointment emails and released-result notifications that are unsent or failed. Future reminders remain scheduled, and sent messages are excluded.</p>'
            '<form action="/admin/email-settings/retry-confirmations" method="post" onsubmit="return confirm(\'Send eligible due appointment emails now?\');">'
            f'<label for="smtp_retry_admin_email">Admin email</label><input id="smtp_retry_admin_email" type="email" name="email" value="{escape(email_value, quote=True)}" required>'
            '<label for="smtp_retry_admin_password">Admin password</label><input id="smtp_retry_admin_password" type="password" name="password" required>'
            '<button class="button" type="submit">Retry due appointment emails</button></form></section>'
        )
    return page(
        "Email Settings",
        f"""<section class="card"><h1>Email Delivery Settings</h1><p>Configure SMTP for appointment confirmations, account notifications, and patient-record alerts. SMTP credentials are encrypted before they are saved.</p>{notice}
        <div class="notice">SMTP credentials are encrypted using a key that the application loads from <code>GAPCC_SMTP_SETTINGS_KEY</code> or stores in <code>.gapcc_smtp_settings_key</code> beside the database. Keep that key file and the database together and private; anyone with both can decrypt saved SMTP credentials. If you use an environment key or move GAPCC to another computer, preserve and restore the original key. Saved settings and eligible queued notifications are loaded and retried when the application starts.</div>
        <form action="/admin/email-settings" method="post">
        <label for="smtp_admin_email">Admin email</label><input id="smtp_admin_email" type="email" name="email" value="{escape(email_value, quote=True)}" required>
        <label for="smtp_admin_password">Admin password</label><input id="smtp_admin_password" type="password" name="password" required>
        <label for="smtp_host">SMTP host</label><input id="smtp_host" name="smtp_host" value="smtp.gmail.com" required>
        <label for="smtp_port">SMTP port</label><input id="smtp_port" type="number" name="smtp_port" value="587" min="1" max="65535" required>
        <label for="smtp_user">SMTP username</label><input id="smtp_user" name="smtp_user" autocomplete="off" required>
        <label for="smtp_secret">SMTP password or app password</label><input id="smtp_secret" type="password" name="smtp_password" autocomplete="new-password" required>
        <label for="smtp_from">From address (optional)</label><input id="smtp_from" type="email" name="smtp_from">
        <button class="button" type="submit">Save encrypted settings</button></form>
        <p>For Gmail, use an App Password, not your normal account password. Environment variables <code>GAPCC_SMTP_HOST</code>, <code>GAPCC_SMTP_PORT</code>, <code>GAPCC_SMTP_USER</code>, <code>GAPCC_SMTP_PASSWORD</code>, and <code>GAPCC_SMTP_FROM</code> override saved values. While GAPCC is running, appointment confirmations, approvals, and updates are emailed when their actions occur; due reminders and released-result notifications are automatically delivered or retried.</p></section>{test_form}
        {retry_form}<a class="button" href="/admin">Back to Admin Console</a>""",
    )


@app.get("/admin/email-settings", response_class=HTMLResponse)
def admin_email_settings_get(request: Request):
    token = request.cookies.get(PORTAL_SESSION_COOKIE, "")
    claim = verify_access_token(token)
    if not claim or claim[0] != "admin":
        return RedirectResponse(url="/admin", status_code=303)
    with connect_db() as connection:
        admin = connection.execute("SELECT email FROM admins WHERE admin_id = ?", (claim[1],)).fetchone()
    if admin is None:
        return RedirectResponse(url="/admin", status_code=303)
    return set_portal_session(
        admin_email_settings_page(email_value=admin["email"]),
        "admin",
        claim[1],
    )


@app.post("/admin/email-settings", response_class=HTMLResponse)
def admin_email_settings_save(
    email: str = Form(...),
    password: str = Form(...),
    smtp_host: str = Form(...),
    smtp_port: int = Form(...),
    smtp_user: str = Form(...),
    smtp_password: str = Form(...),
    smtp_from: str = Form(""),
):
    clean_host = smtp_host.strip()
    clean_user = smtp_user.strip()
    clean_from = smtp_from.strip() or clean_user
    if (
        not clean_host
        or any(character.isspace() for character in clean_host)
        or not clean_user
        or not smtp_password
        or not 1 <= smtp_port <= 65535
        or not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", clean_from)
    ):
        return admin_email_settings_page(
            "Enter a valid SMTP host, port, username, and password.",
            email,
        )
    with connect_db() as connection:
        if verify_admin_credentials(connection, email, password) is None:
            return admin_email_settings_page("Admin access denied. Sign in with valid admin credentials.", email)
    try:
        cipher = smtp_settings_cipher()
    except ValueError:
        return admin_email_settings_page(
            "SMTP settings were not saved. Restore the original .gapcc_smtp_settings_key or configure a valid GAPCC_SMTP_SETTINGS_KEY, then try again.",
            email,
        )
    with connect_db() as connection:
        encrypted = {
            "host": clean_host,
            "port": str(smtp_port),
            "user": clean_user,
            "password": smtp_password,
            "from": clean_from,
        }
        connection.executemany(
            """INSERT INTO smtp_settings (setting_key, encrypted_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET
            encrypted_value = excluded.encrypted_value, updated_at = excluded.updated_at""",
            [
                (key, cipher.encrypt(value.encode("utf-8")).decode("ascii"), utc_now())
                for key, value in encrypted.items()
            ],
        )
    return admin_email_settings_page(
        "SMTP settings were encrypted and saved.",
        email,
        show_test_form=True,
    )


@app.post("/admin/email-settings/test", response_class=HTMLResponse)
def admin_email_settings_test(
    email: str = Form(...),
    password: str = Form(...),
    destination: str = Form(...),
):
    with connect_db() as connection:
        if verify_admin_credentials(connection, email, password) is None:
            return admin_email_settings_page("Admin access denied. Sign in with valid admin credentials.", email)
    status = send_email_message(
        destination.strip(),
        "GAPCC SMTP test",
        "This is a test email from the GAPCC application. SMTP accepted status indicates server acceptance, not inbox delivery.",
    )
    message = {
        "Sent": "The SMTP server accepted the test email. Check the destination inbox and spam folder.",
        "Failed": "The test email could not be sent. Check the server log and SMTP settings.",
        "Not Configured": "Email was not sent because SMTP is not configured.",
    }[status]
    return admin_email_settings_page(
        message,
        email,
        show_test_form=True,
        show_retry_form=status == "Sent",
    )


@app.post("/admin/email-settings/retry-confirmations", response_class=HTMLResponse)
def admin_email_confirmations_retry(email: str = Form(...), password: str = Form(...)):
    with connect_db() as connection:
        if verify_admin_credentials(connection, email, password) is None:
            return admin_email_settings_page("Admin access denied. Sign in with valid admin credentials.", email)
    sent, failed, error_message = dispatch_pending_emails()
    if error_message:
        return admin_email_settings_page(error_message, email)
    summary = f"Appointment email dispatch complete: {sent} accepted by the SMTP server; {failed} not sent. This does not confirm inbox delivery."
    return admin_email_settings_page(summary, email, show_test_form=True)


@app.post("/admin/medications/create", response_class=HTMLResponse)
def create_admin_medication(
    email: str = Form(...),
    password: str = Form(...),
    medication_name: str = Form(...),
    strength: str = Form(""),
    dosage_form: str = Form(""),
    quantity: int = Form(0),
    unit: str = Form("units"),
    notes: str = Form(""),
):
    clean_name = medication_name.strip()
    if not clean_name or quantity < 0:
        return page("Medication Inventory Error", '<section class="card error"><h1>Invalid medication inventory</h1><p>Enter a medication name and a quantity of zero or more.</p><a class="button" href="/admin">Back to Admin Console</a></section>')
    with connect_db() as connection:
        if verify_admin_credentials(connection, email, password) is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><a class="button" href="/admin">Return to Admin</a></section>')
        connection.execute(
            "INSERT INTO clinic_medications (medication_name, strength, dosage_form, quantity, unit, notes, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (clean_name, strength.strip(), dosage_form.strip(), quantity, unit.strip() or "units", notes.strip(), utc_now()),
        )
    return admin_login(email, password)


@app.get("/admin/medications/edit")
def medication_edit_page():
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/medications/edit", response_class=HTMLResponse)
def edit_admin_medication(
    email: str = Form(...),
    password: str = Form(...),
    medication_id: int = Form(...),
    medication_name: str = Form(...),
    strength: str = Form(""),
    dosage_form: str = Form(""),
    quantity: int = Form(...),
    unit: str = Form(...),
    notes: str = Form(""),
):
    clean_name = medication_name.strip()
    if not clean_name or quantity < 0 or not unit.strip():
        return page("Medication Inventory Error", '<section class="card error"><h1>Invalid medication details</h1><p>Enter a medicine name, a unit, and a quantity of zero or more.</p><a class="button" href="/admin">Back to Admin Console</a></section>')
    with connect_db() as connection:
        if verify_admin_credentials(connection, email, password) is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><a class="button" href="/admin">Return to Admin</a></section>')
        medication = connection.execute("SELECT medication_id FROM clinic_medications WHERE medication_id = ?", (medication_id,)).fetchone()
        if medication is None:
            return page("Medication Inventory Error", '<section class="card error"><h1>Medication was not found</h1><a class="button" href="/admin">Back to Admin Console</a></section>')
        connection.execute(
            "UPDATE clinic_medications SET medication_name = ?, strength = ?, dosage_form = ?, quantity = ?, unit = ?, notes = ?, updated_at = ? WHERE medication_id = ?",
            (clean_name, strength.strip(), dosage_form.strip(), quantity, unit.strip(), notes.strip(), utc_now(), medication_id),
        )
    return admin_login(email, password)


@app.post("/admin/medications/delete", response_class=HTMLResponse)
def delete_admin_medication(email: str = Form(...), password: str = Form(...), medication_id: int = Form(...)):
    with connect_db() as connection:
        if verify_admin_credentials(connection, email, password) is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><a class="button" href="/admin">Return to Admin</a></section>')
        connection.execute("DELETE FROM clinic_medications WHERE medication_id = ?", (medication_id,))
    return admin_login(email, password)


@app.post("/admin/packages/create", response_class=HTMLResponse)
def create_admin_package(email: str = Form(...), password: str = Form(...), package_key: str = Form(...), package_name: str = Form(...)):
    clean_key = "".join(character for character in package_key.strip().lower() if character.isalnum() or character in "-_")
    clean_name = package_name.strip()
    with connect_db() as connection:
        if verify_admin_credentials(connection, email, password) is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><a class="button" href="/admin">Return to Admin</a></section>')
        if not clean_key or not clean_name:
            return page("Package Error", '<section class="card error"><h1>Package details are required</h1><a class="button" href="/admin">Back to Admin Console</a></section>')
        connection.execute("INSERT OR IGNORE INTO clinic_packages (package_key, package_name, package_details, created_at, updated_at) VALUES (?, ?, ?, ?, ?)", (clean_key, clean_name, "", utc_now(), utc_now()))
    return admin_login(email, password)


@app.post("/admin/packages/update", response_class=HTMLResponse)
def update_admin_package(email: str = Form(...), password: str = Form(...), package_id: int = Form(...), package_name: str = Form(...), package_details: str = Form("")):
    with connect_db() as connection:
        if verify_admin_credentials(connection, email, password) is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><a class="button" href="/admin">Return to Admin</a></section>')
        connection.execute("UPDATE clinic_packages SET package_name = ?, package_details = ?, updated_at = ? WHERE package_id = ?", (package_name.strip(), package_details.strip(), utc_now(), package_id))
    return admin_login(email, password)


@app.post("/admin/packages/delete", response_class=HTMLResponse)
def delete_admin_package(email: str = Form(...), password: str = Form(...), package_id: int = Form(...)):
    with connect_db() as connection:
        if verify_admin_credentials(connection, email, password) is None:
            return page("Admin Login", '<section class="card error"><h1>Admin access denied</h1><a class="button" href="/admin">Return to Admin</a></section>')
        connection.execute("DELETE FROM clinic_packages WHERE package_id = ?", (package_id,))
    return admin_login(email, password)


@app.post("/admin/search", response_class=HTMLResponse)
def admin_search(email: str = Form(...), password: str = Form(...), q: str = Form(...)):
    query = q.strip()
    with connect_db() as connection:
        admin = verify_admin_credentials(connection, email, password)
        if admin is None:
            return page("Admin Search", '<section class="card error"><h1>Admin access denied</h1><p>Sign in again to search database records.</p></section>')
        like = f"%{query}%"
        patients = connection.execute("SELECT patient_id, given_name, middle_name, last_name, email FROM patients WHERE patient_id LIKE ? OR given_name LIKE ? OR middle_name LIKE ? OR last_name LIKE ? OR email LIKE ? ORDER BY last_name", (like, like, like, like, like)).fetchall()
        bookings = connection.execute("SELECT b.token_id, b.service, b.appointment_date, p.given_name, p.middle_name, p.last_name FROM bookings b JOIN patients p ON p.patient_id = b.patient_id WHERE b.token_id LIKE ? OR b.service LIKE ? OR p.given_name LIKE ? OR p.last_name LIKE ? ORDER BY b.appointment_date DESC", (like, like, like, like)).fetchall()
        results = connection.execute("SELECT l.test_name, l.result_summary, l.result_status, p.given_name, p.middle_name, p.last_name FROM lab_results l JOIN patients p ON p.patient_id = l.patient_id WHERE l.test_name LIKE ? OR l.result_summary LIKE ? OR p.given_name LIKE ? OR p.last_name LIKE ? ORDER BY l.result_id DESC", (like, like, like, like)).fetchall()
    patient_items = "".join(f"<li><strong>{escape(patient_name(row))}</strong> - {escape(row['patient_id'])}, {escape(row['email'])}</li>" for row in patients) or "<li>No patient matches.</li>"
    booking_items = "".join(f"<li><strong>{escape(row['token_id'])}</strong> - {escape(patient_name(row))}, {escape(row['service'])}, {escape(row['appointment_date'])}</li>" for row in bookings) or "<li>No booking matches.</li>"
    result_items = "".join(f"<li><strong>{escape(patient_name(row))}</strong> - {escape(row['test_name'])}: {escape(row['result_summary'])} ({escape(row['result_status'])})</li>" for row in results) or "<li>No result matches.</li>"
    return page("Admin Search", f"<section class=\"card\"><h1>Admin Database Search</h1><p>Results for <strong>{escape(query)}</strong></p><h2>Patients</h2><ul>{patient_items}</ul><h2>Bookings</h2><ul>{booking_items}</ul><h2>Laboratory Results</h2><ul>{result_items}</ul><a class=\"button\" href=\"/admin\">Back to Admin Console</a></section>")


@app.get("/doctor", response_class=HTMLResponse)
def doctor_page(request: Request):
    token = request.cookies.get(PORTAL_SESSION_COOKIE, "")
    claim = verify_access_token(token)
    if claim and claim[0] == "doctor":
        return doctor_login("", "", session_token=token)
    return page(
        "Doctor Workspace",
        """<section class="card"><h1>Doctor Workspace</h1><p>Review appointments and publish patient records or laboratory results.</p>
        <form action="/doctor" method="post"><div class="grid"><div><label for="email">Doctor Email</label><input id="email" type="email" name="email" value="doctor@gapcc.local" autocomplete="username" required></div><div><label for="password">Password</label><input id="password" type="password" name="password" autocomplete="current-password" required></div></div><button class="button" type="submit">Log In to Doctor Dashboard</button></form><p class="entry-signup"><a href="/account-recover/doctor">Forgot password?</a></p><p class="entry-signup">Don't have an account? <a href="/account-request/doctor">Create account</a> · <a href="/account-request/doctor/status">Check request status</a></p><a class="entry-home" href="/">Back to GAPCC Home</a></section>""",
        page_class="entry-shell entry-doctor",
    )


@app.get("/doctor/logout")
def doctor_portal_logout():
    response = RedirectResponse(url="/doctor", status_code=303)
    return clear_portal_session(response)


@app.post("/doctor", response_class=HTMLResponse)
def doctor_login(
    email: str = Form(""),
    password: str = Form(""),
    session_token: Annotated[str, Form()] = "",
):
    with connect_db() as connection:
        if session_token:
            claim = verify_access_token(session_token)
            doctor = connection.execute(
                "SELECT * FROM doctors WHERE doctor_id = ? AND active = 1",
                (claim[1],),
            ).fetchone() if claim and claim[0] == "doctor" else None
        else:
            doctor = connection.execute(
                "SELECT * FROM doctors WHERE email = ? AND password_hash = ? AND active = 1",
                (email.strip().lower(), token_hash(password)),
            ).fetchone()
        if doctor is None:
            return page("Doctor Login", '<section class="card error"><h1>Doctor access denied</h1><p>Check the doctor email and password.</p><a class="button" href="/doctor">Try Again</a></section>')
        schedules = connection.execute(
            "SELECT day_of_week, start_time, end_time FROM doctor_schedules WHERE doctor_id = ? AND active = 1 ORDER BY schedule_id",
            (doctor["doctor_id"],),
        ).fetchall()
        bookings = connection.execute(
            """SELECT b.booking_id, b.patient_id, b.service, b.facility, b.appointment_date,
            p.given_name, p.middle_name, p.last_name FROM bookings b JOIN patients p ON p.patient_id = b.patient_id
            WHERE b.appointment_date >= ? ORDER BY b.appointment_date, b.booking_id""",
            (datetime.now(timezone.utc).date().isoformat(),),
        ).fetchall()
        patients = connection.execute("SELECT patient_id, given_name, middle_name, last_name, gender, date_of_birth FROM patients ORDER BY last_name, given_name").fetchall()
        residents = connection.execute("SELECT display_name, email, specialization, facility, registration_number, residency_program, residency_year, active FROM doctors WHERE doctor_role = 'Resident Doctor' ORDER BY display_name").fetchall()
        pending_doctor_messages = connection.execute(
            """SELECT COUNT(*) AS total FROM appointment_messages m
            JOIN bookings b ON b.booking_id = m.booking_id
            WHERE b.doctor_id = ? AND m.status IN ('Queued', 'Failed')""",
            (doctor["doctor_id"],),
        ).fetchone()["total"]
        own_results = connection.execute(
            """SELECT l.result_id, l.patient_id, l.test_name, l.collected_at, p.given_name, p.middle_name, p.last_name
            FROM lab_results l JOIN patients p ON p.patient_id = l.patient_id
            WHERE l.doctor_id = ? AND l.pdf_data IS NOT NULL ORDER BY l.collected_at DESC, l.result_id DESC LIMIT 25""",
            (doctor["doctor_id"],),
        ).fetchall()
    patient_options_html = "".join(f"<option value=\"{escape(patient['patient_id'], quote=True)}\">{escape(patient['patient_id'])} - {escape(patient_name(patient))}</option>" for patient in patients)
    results_panel = doctor_results_panel(doctor, patient_options_html, own_results, sign_access_token("doctor", doctor["doctor_id"]))
    schedule_text = ", ".join(f"{row['day_of_week']} {row['start_time']}-{row['end_time']}" for row in schedules) or "No schedule published"
    booking_rows = "".join(
        f"<tr><td>{escape(booking['appointment_date'])}</td><td>{escape(patient_name(booking))}</td><td>{escape(booking['service'])}</td><td>{escape(booking['facility'])}</td></tr>"
        for booking in bookings
    ) or "<tr><td colspan=4>No upcoming appointments.</td></tr>"
    patient_options = "".join(f"<option value=\"{escape(patient['patient_id'], quote=True)}\">{escape(patient_name(patient))} ({escape(patient['patient_id'])})</option>" for patient in patients)
    resident_rows = "".join(f"<tr><td>{escape(resident['display_name'])}</td><td>{escape(resident['specialization'])}</td><td>{escape(resident['residency_program'])} · Year {escape(resident['residency_year'])}</td><td>{escape(resident['registration_number'])}</td><td>{escape(resident['facility'])}</td><td>{'Active' if resident['active'] else 'Inactive'}</td></tr>" for resident in residents) or "<tr><td colspan=6>No resident doctors registered yet.</td></tr>"
    def render_doctor_patient_row(patient):
        age = patient_age(patient["date_of_birth"])
        age_group = (
            "child" if isinstance(age, int) and age < 18
            else "adult" if isinstance(age, int) and age < 60
            else "senior" if isinstance(age, int)
            else "unknown"
        )
        age_display = f"{age} years" if isinstance(age, int) else "Not provided"
        return f"<tr data-age-group=\"{age_group}\" data-gender=\"{escape(patient['gender'], quote=True)}\"><td>{escape(patient['patient_id'])}</td><td>{escape(patient_name(patient))}</td><td>{escape(patient['gender'])}</td><td>{age_display}</td></tr>"

    patient_rows = "".join(render_doctor_patient_row(patient) for patient in patients) or "<tr><td colspan=4>No patients registered yet.</td></tr>"
    doctor_gender_filter_options = "".join(
        f'<option value="{escape(gender, quote=True)}">{escape(gender)}</option>'
        for gender in GENDER_OPTIONS
    )
    appointments_today = sum(
        booking["appointment_date"] == datetime.now(timezone.utc).date().isoformat()
        for booking in bookings
    )
    doctor_count_bar = "<nav class=\"dashboard-counts\" id=\"doctor-dashboard-counts\" aria-label=\"Doctor dashboard counts\">" + "".join(
        [
            dashboard_count_link("#doctor-appointments", "!", "Notifications and alerts", appointments_today, "alert-count"),
            dashboard_count_link("#doctor-appointments", "✉", "Messages", pending_doctor_messages, "message-count"),
            dashboard_count_link("#doctor-appointments", "▦", "Bookings", len(bookings), "booking-count"),
            dashboard_count_link("#doctor-profile", "◷", "Schedules", len(schedules), "schedule-count"),
        ]
    ) + "</nav>"
    doctor_profile_fields = [
        ("Doctor ID", doctor["doctor_id"]),
        ("Email", doctor["email"]),
        ("Role", doctor["doctor_role"]),
        ("Specialization", doctor["specialization"]),
        ("Facility", doctor["facility"]),
        ("Schedule", schedule_text),
        ("License number", doctor["registration_number"] or "Not provided"),
    ]
    if doctor["doctor_role"] == "Resident Doctor":
        doctor_profile_fields.extend(
            [("Residency program", doctor["residency_program"] or "Not provided"), ("Residency year", doctor["residency_year"] or "Not provided")]
        )
    doctor_profile_grid = "".join(
        f'<div class="profile-field"><span>{escape(label)}</span><strong>{escape(value)}</strong></div>'
        for label, value in doctor_profile_fields
    )
    doctor_name = doctor["display_name"]
    doctor_initial = escape(doctor_name[:1].upper() or "D")
    doctor_profile = f"""<section class="portal-profile" aria-labelledby="doctor-profile-title"><div class="profile-avatar" aria-hidden="true">{doctor_initial}</div><div class="profile-main"><div class="profile-heading"><p class="profile-label">Doctor profile</p><span class="profile-badge">Active</span></div><h2 id="doctor-profile-title">{escape(doctor_name)}</h2><div class="profile-grid">{doctor_profile_grid}</div></div></section>"""
    doctor_profile += doctor_count_bar
    doctor_session_token = sign_access_token("doctor", doctor["doctor_id"], PORTAL_SESSION_TTL)
    return set_portal_session(page(
        "Doctor Workspace",
        f"""<style>{DOCTOR_DASHBOARD_CSS}</style><div class="doctor-dashboard"><aside class="doctor-sidebar"><a class="doctor-brand" href="/"><span class="doctor-brand-mark">+</span>GAPCC Clinical</a><div class="doctor-sidebar-profile"><span class="profile-avatar" aria-hidden="true">{doctor_initial}</span><strong>{escape(doctor_name)}</strong><small>{escape(doctor['doctor_role'])}</small></div><nav class="doctor-nav" aria-label="Doctor workspace"><a href="#doctor-overview">Dashboard</a><a href="#doctor-appointments">Appointments</a><a href="#doctor-profile">Doctor Profile</a><a href="#doctor-patients">Patients</a><a href="#doctor-residents">Resident Doctors</a><a href="#doctor-records">Patient Records</a><a href="#doctor-results">Lab Results</a></nav><a class="doctor-home-link" href="/">Return to GAPCC Home</a></aside><div class="doctor-dashboard-main"><header class="doctor-topbar"><p>Clinical workspace / Dashboard</p><strong>{escape(doctor['facility'])}</strong></header><section class="doctor-welcome" id="doctor-overview"><div><p class="eyebrow">Medical dashboard</p><h1>Good day, {escape(doctor_name)}</h1><p>{escape(doctor['specialization'])} · {escape(doctor['doctor_role'])}</p></div><div class="welcome-schedule"><strong>Today's schedule</strong><br>{escape(schedule_text)}</div></section><div class="doctor-stats"><article class="doctor-stat"><span class="stat-icon">A</span><div><strong>{appointments_today}</strong><span>Appointments today</span></div></article><article class="doctor-stat"><span class="stat-icon">↗</span><div><strong>{len(bookings)}</strong><span>Upcoming appointments</span></div></article><article class="doctor-stat"><span class="stat-icon">P</span><div><strong>{len(patients)}</strong><span>Patients in directory</span></div></article></div><div class="doctor-panels"><section class="doctor-panel" id="doctor-appointments"><div class="panel-heading"><h2>Upcoming Appointments</h2><small>{len(bookings)} scheduled</small></div><form class="doctor-search" action="/doctor/search" method="post"><input type="hidden" name="email" value="{escape(doctor['email'], quote=True)}"><input type="hidden" name="password" value="{escape(doctor_session_token, quote=True)}"><input name="q" aria-label="Doctor record search" placeholder="Search patient, service, or result" required><button class="button" type="submit">Search</button></form><div class="table-scroll"><table><thead><tr><th>Date</th><th>Patient</th><th>Service</th><th>Facility</th></tr></thead><tbody>{booking_rows}</tbody></table></div></section><section class="doctor-panel doctor-profile-panel" id="doctor-profile"><div class="panel-heading"><h2>Doctor Profile</h2><small>Account details</small></div>{doctor_profile}</section></div><section class="doctor-panel" id="doctor-patients"><div class="panel-heading"><h2>Patient Directory</h2><small>{len(patients)} records</small></div><div class="doctor-patient-filters" aria-label="Filter patients"><strong>Filter</strong><label>Gender<select data-doctor-patient-gender-filter aria-label="Filter patients by gender"><option value="">All genders</option>{doctor_gender_filter_options}</select></label><label>Age Group<select data-doctor-patient-age-filter aria-label="Filter patients by age group"><option value="">All ages</option><option value="child">Children (under 18)</option><option value="adult">Adults (18-59)</option><option value="senior">Senior citizens (60+)</option></select></label></div><div class="table-scroll"><table data-doctor-patient-directory><thead><tr><th>Patient ID</th><th>Patient Name</th><th>Gender</th><th>Age</th></tr></thead><tbody>{patient_rows}</tbody></table></div><script>{DOCTOR_PATIENT_FILTER_SCRIPT}</script></section><div class="doctor-forms"><section class="doctor-panel" id="doctor-residents"><div class="panel-heading"><h2>Register Resident Doctor</h2><small>Clinical roster</small></div><form action="/doctor/residents/register" method="post"><input type="hidden" name="registering_doctor_id" value="{escape(doctor['doctor_id'], quote=True)}"><div class="grid"><div><label>Resident Full Name</label><input name="display_name" required></div><div><label>Email</label><input type="email" name="email" required></div><div><label>Temporary Password</label><input type="password" name="password" required></div><div><label>Specialization</label><input name="specialization" placeholder="Family Medicine" required></div><div><label>Facility</label><input name="facility" value="{escape(doctor['facility'], quote=True)}" required></div><div><label>Registration Number</label><input name="registration_number" required></div><div><label>Residency Program</label><input name="residency_program" placeholder="Internal Medicine" required></div><div><label>Residency Year</label><select name="residency_year"><option>1</option><option>2</option><option>3</option><option>4</option></select></div></div><button class="button green" type="submit">Register Resident Doctor</button></form><h2>Resident Doctor Roster</h2><div class="table-scroll"><table><thead><tr><th>Name</th><th>Specialization</th><th>Program / Year</th><th>Registration #</th><th>Facility</th><th>Status</th></tr></thead><tbody>{resident_rows}</tbody></table></div></section><section class="doctor-panel" id="doctor-records"><div class="panel-heading"><h2>Publish Patient Record</h2><small>Clinical notes</small></div><form action="/doctor/records" method="post"><input type="hidden" name="doctor_id" value="{escape(doctor['doctor_id'], quote=True)}"><div class="grid"><div><label for="record_patient">Patient</label><select id="record_patient" name="patient_id" required>{patient_options}</select></div><div><label for="record_type">Record Type</label><input id="record_type" name="record_type" list="doctor-record-types" value="Consultation Note" required><datalist id="doctor-record-types"><option value="Consultation Note"><option value="Prescription"></datalist></div></div><label for="record_note">Clinical Note</label><textarea id="record_note" name="note" rows="4" required></textarea><button class="button" type="submit">Save Patient Record</button></form></section>{results_panel}</div></div></div>""",
    ), "doctor", doctor["doctor_id"])


@app.post("/doctor/search", response_class=HTMLResponse)
def doctor_search(email: str = Form(...), password: str = Form(...), q: str = Form(...)):
    query = q.strip()
    with connect_db() as connection:
        claim = verify_access_token(password)
        if claim:
            doctor = connection.execute(
                "SELECT doctor_id, display_name FROM doctors WHERE doctor_id = ? AND active = 1",
                (claim[1],),
            ).fetchone() if claim[0] == "doctor" else None
        else:
            doctor = connection.execute(
                "SELECT doctor_id, display_name FROM doctors WHERE email = ? AND password_hash = ? AND active = 1",
                (email.strip().lower(), token_hash(password)),
            ).fetchone()
        if doctor is None:
            return page("Doctor Search", '<section class="card error"><h1>Doctor access denied</h1><p>Sign in again to search care records.</p></section>')
        like = f"%{query}%"
        patients = connection.execute("SELECT patient_id, given_name, middle_name, last_name, email FROM patients WHERE patient_id LIKE ? OR given_name LIKE ? OR last_name LIKE ? OR email LIKE ? ORDER BY last_name", (like, like, like, like)).fetchall()
        bookings = connection.execute("SELECT b.token_id, b.service, b.appointment_date, p.given_name, p.middle_name, p.last_name FROM bookings b JOIN patients p ON p.patient_id = b.patient_id WHERE b.service LIKE ? OR b.token_id LIKE ? OR p.given_name LIKE ? OR p.last_name LIKE ? ORDER BY b.appointment_date DESC", (like, like, like, like)).fetchall()
    patient_items = "".join(f"<li><strong>{escape(patient_name(row))}</strong> - {escape(row['patient_id'])}, {escape(row['email'])}</li>" for row in patients) or "<li>No patient matches.</li>"
    booking_items = "".join(f"<li><strong>{escape(row['token_id'])}</strong> - {escape(patient_name(row))}, {escape(row['service'])}, {escape(row['appointment_date'])}</li>" for row in bookings) or "<li>No booking matches.</li>"
    return page("Doctor Search", f"<section class=\"card\"><h1>Doctor Care Search</h1><p>Results for <strong>{escape(query)}</strong></p><h2>Patients</h2><ul>{patient_items}</ul><h2>Bookings</h2><ul>{booking_items}</ul><a class=\"button\" href=\"/doctor\">Back to Doctor Workspace</a></section>")


@app.post("/doctor/residents/register", response_class=HTMLResponse)
def register_resident_doctor(
    registering_doctor_id: str = Form(...),
    display_name: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    specialization: str = Form(...),
    facility: str = Form(...),
    registration_number: str = Form(...),
    residency_program: str = Form(...),
    residency_year: str = Form(...),
):
    with connect_db() as connection:
        registering_doctor = connection.execute("SELECT doctor_id FROM doctors WHERE doctor_id = ? AND active = 1", (registering_doctor_id,)).fetchone()
        if registering_doctor is None:
            return page("Resident Registration Error", '<section class="card error"><h1>Registration not authorized</h1><p>Only an active doctor may register a resident doctor.</p><a class="button" href="/doctor">Return to Doctor Workspace</a></section>')
        if not password_meets_policy(password):
            return page("Resident Registration Error", f'<section class="card error"><h1>Password does not meet requirements</h1><p>{escape(PASSWORD_POLICY_MESSAGE)}</p><a class="button" href="/doctor">Return to Doctor Workspace</a></section>')
        try:
            connection.execute("INSERT INTO doctors (doctor_id, email, password_hash, display_name, specialization, facility, doctor_role, registration_number, residency_program, residency_year, created_at) VALUES (?, ?, ?, ?, ?, ?, 'Resident Doctor', ?, ?, ?, ?)", (f"RES-{secrets.token_hex(5).upper()}", email.strip().lower(), token_hash(password), display_name.strip(), specialization.strip(), facility.strip(), registration_number.strip(), residency_program.strip(), residency_year.strip(), utc_now()))
        except sqlite3.IntegrityError:
            return page("Resident Registration Error", '<section class="card error"><h1>Resident doctor could not be registered</h1><p>That email address is already registered.</p><a class="button" href="/doctor">Return to Doctor Workspace</a></section>')
    return page("Resident Doctor Registered", '<section class="card"><h1>Resident doctor registered</h1><p>The resident has been added to the GAPCC doctor roster.</p><a class="button" href="/doctor">Return to Doctor Workspace</a></section>')


@app.post("/doctor/records", response_class=HTMLResponse)
def create_patient_record(doctor_id: str = Form(...), patient_id: str = Form(...), record_type: str = Form(...), note: str = Form(...)):
    with connect_db() as connection:
        doctor = connection.execute("SELECT doctor_id, display_name FROM doctors WHERE doctor_id = ? AND active = 1", (doctor_id,)).fetchone()
        patient = connection.execute("SELECT patient_id, email FROM patients WHERE patient_id = ?", (patient_id,)).fetchone()
        if doctor is None or patient is None or not note.strip():
            return page("Record Error", '<section class="card error"><h1>Record could not be saved</h1><p>Select a valid doctor, patient, and clinical note.</p></section>')
        connection.execute("INSERT INTO patient_records (patient_id, doctor_id, record_type, note, created_at) VALUES (?, ?, ?, ?, ?)", (patient_id, doctor_id, record_type.strip(), note.strip(), utc_now()))
    notification_body = f"A new consultation record has been added to your GAPCC patient portal by {doctor['display_name']}. For your privacy, the clinical details are not included in this email. Sign in to your patient portal to review the record."
    delivery_status = send_email_message(patient["email"], "GAPCC consultation record available", notification_body)
    delivery_message = {
        "Sent": f"A notification was emailed to {patient['email']}. The clinical details remain in the patient portal.",
        "Failed": f"The record was saved, but its email notification could not be delivered to {patient['email']}.",
        "Not Configured": "The record was saved, but Gmail notifications are not configured. The record remains in the patient portal.",
    }[delivery_status]
    return page("Record Saved", f'<section class="card"><h1>Patient record saved</h1><p>{escape(delivery_message)}</p><a class="button" href="/doctor">Return to Doctor Workspace</a></section>')


@app.post("/doctor/license", response_class=HTMLResponse)
def save_doctor_license(doctor_token: str = Form(...), registration_number: str = Form(...)):
    claim = verify_access_token(doctor_token)
    license_no = registration_number.strip()[:40]
    if claim is None or claim[0] != "doctor" or not license_no:
        return page("License Error", '<section class="card error"><h1>License number not saved</h1><p>Your session expired or the license number is empty. Sign in again.</p><a class="button" href="/doctor">Doctor Log In</a></section>')
    with connect_db() as connection:
        connection.execute("UPDATE doctors SET registration_number = ? WHERE doctor_id = ? AND active = 1", (license_no, claim[1]))
    return page("License Saved", '<section class="card"><h1>License number saved</h1><p>Sign in again to upload results with your watermark.</p><a class="button" href="/doctor">Return to Doctor Workspace</a></section>')


@app.post("/doctor/results", response_class=HTMLResponse)
async def create_lab_result(
    doctor_token: str = Form(...),
    patient_id: str = Form(...),
    test_name: str = Form(...),
    result_summary: str = Form(...),
    result_file: Annotated[UploadFile | None, File()] = None,
):
    def fail(title, message):
        return page("Result Error", f'<section class="card error"><h1>{escape(title)}</h1><p>{escape(message)}</p><a class="button" href="/doctor">Return to Doctor Workspace</a></section>')

    claim = verify_access_token(doctor_token)
    if claim is None or claim[0] != "doctor":
        return fail("Session expired", "Sign in to the Doctor Portal again before uploading.")
    if result_file is None or not result_file.filename:
        return fail("No file selected", "Choose a PDF, PNG, or JPEG result to upload.")
    data = await result_file.read(MAX_RESULT_BYTES + 1)
    kind, problem = validate_result_upload(data)
    if kind is None:
        return fail("File rejected", problem)
    with connect_db() as connection:
        doctor = connection.execute("SELECT doctor_id, display_name, registration_number FROM doctors WHERE doctor_id = ? AND active = 1", (claim[1],)).fetchone()
        patient = connection.execute("SELECT patient_id, email FROM patients WHERE patient_id = ?", (patient_id.strip(),)).fetchone()
        if doctor is None or patient is None or not test_name.strip() or not result_summary.strip():
            return fail("Result could not be saved", "Select a valid patient and provide the result details.")
        if not (doctor["registration_number"] or "").strip():
            return fail("License number required", "Add your PRC / license number in the upload panel first; it is printed in the watermark.")
        cursor = connection.execute(
            "INSERT INTO lab_results (patient_id, doctor_id, test_name, result_summary, result_status, collected_at, released_at, pdf_filename, pdf_data, file_mime, created_at) VALUES (?, ?, ?, ?, 'Released', ?, ?, ?, ?, ?, ?)",
            (patient["patient_id"], doctor["doctor_id"], test_name.strip(), result_summary.strip(), datetime.now(timezone.utc).date().isoformat(), utc_now(), Path(result_file.filename).name[:120], data, RESULT_MIME[kind], utc_now()),
        )
        result_id = cursor.lastrowid
        log_result_access(connection, result_id, "doctor", doctor["doctor_id"], "upload")
        notification_created_at = utc_now()
        connection.execute(
            """INSERT INTO lab_result_notifications
            (result_id, destination, status, scheduled_for, sent_at, created_at)
            VALUES (?, ?, 'Pending', ?, NULL, ?)""",
            (
                result_id,
                patient["email"],
                notification_created_at,
                notification_created_at,
            ),
        )
    _, _, dispatch_error = dispatch_pending_emails()
    if dispatch_error:
        logging.getLogger(__name__).warning("Lab-result email remains queued: %s", dispatch_error)
    with connect_db() as connection:
        notification = connection.execute(
            "SELECT status FROM lab_result_notifications WHERE result_id = ?",
            (result_id,),
        ).fetchone()
    email_status = notification["status"]
    email_notice = {
        "Sent": f"A result-available notification was sent to {patient['email']}.",
        "Not Configured": "The result was released and its email notification is queued until SMTP is configured.",
        "Failed": "The result was released; notification email failed and will be retried automatically.",
        "Pending": "The result was released and its email notification is queued for automatic delivery.",
        "Queued": "The result was released and its email notification is queued for automatic delivery.",
    }[email_status]
    links = result_action_links(result_id, doctor_token)
    return page(
        "Result Released",
        f'<style>{RESULT_ACTION_CSS}</style><section class="card"><h1>Result uploaded and released</h1><p>The watermarked document is ready. {escape(email_notice)} The patient can view, download, and print it from their portal.</p>'
        f'<p><a class="button green" href="/results/{result_id}/view?t={escape(doctor_token, quote=True)}&amp;print=1" target="_blank" rel="noopener">Print now</a> {links}</p>'
        '<a class="button" href="/doctor">Return to Doctor Workspace</a></section>',
    )


@app.get("/results/{result_id}/file")
def result_file_download(result_id: int, t: str = "", dl: int = 0):
    denied = HTMLResponse("Access denied.", status_code=403, headers=SECURITY_HEADERS)
    with connect_db() as connection:
        result = load_result_row(connection, result_id)
        access = authorize_result_access(connection, t, result)
        if access is None:
            return denied
        log_result_access(connection, result_id, access[0], access[1], "download" if dl else "view")
    document = build_watermarked_result(result)
    disposition = "attachment" if dl else "inline"
    headers = dict(SECURITY_HEADERS)
    headers["Content-Disposition"] = f'{disposition}; filename="GAPCC-Result-{result["patient_id"]}-{result_id}.pdf"'
    return Response(content=document, media_type="application/pdf", headers=headers)


@app.get("/results/{result_id}/view")
def result_viewer(result_id: int, t: str = "", print: int = 0):
    with connect_db() as connection:
        result = load_result_row(connection, result_id)
        access = authorize_result_access(connection, t, result)
        if access is None:
            return HTMLResponse("Access denied.", status_code=403, headers=SECURITY_HEADERS)
        watermark = doctor_watermark_text(result["doctor_name"], result["license_no"])
    safe = escape(t, quote=True)
    html = (
        RESULT_VIEWER_HTML.replace("__ID__", str(result_id))
        .replace("__WATERMARK__", escape(watermark))
        .replace("__FILE__", f"/results/{result_id}/file?t={safe}")
        .replace("__DOWNLOAD__", f"/results/{result_id}/file?t={safe}&amp;dl=1")
        .replace("__AUTOPRINT__", "true" if print else "false")
    )
    return HTMLResponse(html, headers=SECURITY_HEADERS)


@app.get("/patient/results/{result_id}/pdf")
def legacy_patient_result(result_id: int, t: str = ""):
    return RedirectResponse(f"/results/{result_id}/file?t={t}", status_code=303)


@app.post("/lab-results/{portal}", response_class=HTMLResponse)
def lab_results(portal: str, reference: str = Form(...)):
    if portal not in {"patient", "doctor"}:
        return HTMLResponse("Invalid results portal.", status_code=404)
    role = "Patient" if portal == "patient" else "Doctor"
    clean_reference = escape(reference.strip())
    return page("Laboratory Results", f"<section class=\"card\"><h1>{role} Laboratory Results Portal</h1><p>Lookup reference <strong>{clean_reference}</strong> was received.</p></section>")
