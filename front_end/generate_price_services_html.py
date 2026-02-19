#!/usr/bin/env python3
import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path


def clean(text: str) -> str:
    return (text or "").strip()


def first_non_empty(*values: str) -> str:
    for v in values:
        c = clean(v)
        if c:
            return c
    return ""


def parse_rate(raw: str):
    raw = clean(raw)
    if not raw:
        return None
    try:
        return float(raw.replace(",", ""))
    except ValueError:
        return None


def parse_npi(raw: str):
    raw = clean(raw)
    if not raw:
        return []
    tokens = re.findall(r"\d+", raw)
    return [t for t in tokens if t]


def short_service_name(description: str, fallback: str) -> str:
    base = first_non_empty(description, fallback, "Unknown service")
    return base[:90].rstrip() + ("..." if len(base) > 90 else "")


def address_from_row(row: dict) -> str:
    place = first_non_empty(row.get("practice_location", ""))
    if place:
        return place
    parts = [
        clean(row.get("practice_addr_line1", "")),
        clean(row.get("practice_addr_line2", "")),
        clean(row.get("practice_city", "")),
        clean(row.get("practice_state", "")),
        clean(row.get("practice_postal_code", "")),
    ]
    return ", ".join([p for p in parts if p])


def safe_json(data) -> str:
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"))


def build_dataset(csv_path: Path, max_services: int, max_hospitals: int):
    provider_service_rates = defaultdict(lambda: defaultdict(list))
    provider_service_settings = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: {"rates": [], "npis": set()}))
    )
    provider_address = {}
    provider_distance = {}
    service_meta = {}

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rate = parse_rate(row.get("negotiated_rate", ""))
            if rate is None:
                continue

            provider_name = first_non_empty(
                row.get("provider_name", ""),
                row.get("provider_org_name", ""),
                row.get("name", ""),
                row.get("source_file_provider", ""),
            )
            if not provider_name:
                continue

            code = first_non_empty(row.get("billing_code", "UNKNOWN"))
            service_key = f"{code}|{first_non_empty(row.get('description', ''), row.get('name', ''), 'Unknown service')}"
            service_meta.setdefault(
                service_key,
                {
                    "code": code,
                    "name": short_service_name(row.get("description", ""), row.get("name", "")),
                },
            )

            provider_service_rates[provider_name][service_key].append(rate)

            setting_key = (
                first_non_empty(row.get("negotiation_arrangement", ""), "unknown"),
                first_non_empty(row.get("billing_class", ""), "unknown"),
                first_non_empty(row.get("negotiated_type", ""), "unknown"),
            )
            setting = provider_service_settings[provider_name][service_key][setting_key]
            setting["rates"].append(rate)
            for npi in parse_npi(row.get("npi", "")):
                setting["npis"].add(npi)

            provider_address.setdefault(provider_name, address_from_row(row) or "Address not listed")
            provider_distance.setdefault(
                provider_name, f"{(len(provider_distance) % 23) + 1}.0 mi"
            )

    coverage = defaultdict(int)
    for provider_name, svc_rates in provider_service_rates.items():
        for svc_key in svc_rates:
            if svc_rates[svc_key]:
                coverage[svc_key] += 1

    selected_service_keys = [
        key
        for key, _ in sorted(
            coverage.items(),
            key=lambda kv: (-kv[1], service_meta[kv[0]]["code"], service_meta[kv[0]]["name"]),
        )[:max_services]
    ]

    services = []
    service_id_by_key = {}
    for i, svc_key in enumerate(selected_service_keys, start=1):
        sid = f"s{i}"
        service_id_by_key[svc_key] = sid
        services.append(
            {
                "id": sid,
                "name": service_meta[svc_key]["name"],
                "code": service_meta[svc_key]["code"],
            }
        )

    provider_rows = []
    for provider_name, svc_rates in provider_service_rates.items():
        low_sum = 0.0
        high_sum = 0.0
        present = 0
        for svc_key in selected_service_keys:
            rates = svc_rates.get(svc_key, [])
            if not rates:
                continue
            present += 1
            low_sum += min(rates)
            high_sum += max(rates)
        if present == 0:
            continue
        provider_rows.append(
            {
                "name": provider_name,
                "present": present,
                "low_sum": low_sum,
                "high_sum": high_sum,
                "mid_sum": (low_sum + high_sum) / 2.0,
            }
        )

    provider_rows.sort(key=lambda row: (-row["present"], row["mid_sum"], row["name"]))
    provider_rows = provider_rows[:max_hospitals]
    provider_rows.sort(key=lambda row: (row["mid_sum"], row["name"]))

    hospitals = []
    for idx, row in enumerate(provider_rows, start=1):
        provider_name = row["name"]
        services_payload = {}
        for svc_key in selected_service_keys:
            svc_id = service_id_by_key[svc_key]
            rates = provider_service_rates[provider_name].get(svc_key, [])
            if not rates:
                continue
            settings = []
            setting_rows = provider_service_settings[provider_name][svc_key]
            for setting_idx, (setting_key, setting_data) in enumerate(
                sorted(
                    setting_rows.items(),
                    key=lambda kv: (
                        min(kv[1]["rates"]) if kv[1]["rates"] else float("inf"),
                        kv[0][0],
                        kv[0][1],
                        kv[0][2],
                    )
                )[:8],
                start=1,
            ):
                arrangement, billing_class, negotiated_type = setting_key
                setting_rates = setting_data["rates"]
                settings.append(
                    {
                        "setting": f"Setting {chr(64 + setting_idx)}",
                        "arrangement": arrangement,
                        "billingClass": billing_class,
                        "negotiatedType": negotiated_type,
                        "low": round(min(setting_rates)),
                        "high": round(max(setting_rates)),
                        "npi": len(setting_data["npis"]) or 1,
                    }
                )

            services_payload[svc_id] = {
                "low": round(min(rates)),
                "high": round(max(rates)),
                "note": f"({len(setting_rows)} settings)",
                "settings": settings,
            }

        col = (idx - 1) % 5
        row_idx = (idx - 1) // 5
        pin_x = max(10, min(90, 14 + col * 18 + (row_idx % 2) * 4))
        pin_y = max(12, min(88, 28 + row_idx * 17))
        hospitals.append(
            {
                "id": f"h{idx}",
                "rank": idx,
                "name": provider_name,
                "address": provider_address.get(provider_name, "Address not listed"),
                "distance": provider_distance.get(provider_name, f"{idx}.0 mi"),
                "pin": {"x": pin_x, "y": pin_y},
                "services": services_payload,
            }
        )

    return services, hospitals


def render_html(template_html: str, services, hospitals):
    script_services = f"const services = {safe_json(services)};"
    script_hospitals = f"const hospitals = {safe_json(hospitals)};"

    template_html = re.sub(
        r"<title>.*?</title>",
        "<title>Unraveled - Price Services (Generated)</title>",
        template_html,
        count=1,
        flags=re.IGNORECASE | re.DOTALL,
    )
    template_html = re.sub(
        r"<div class=\"sub\">.*?</div>",
        f"<div class=\"sub\"><b>{len(services)} services</b> and <b>{len(hospitals)} providers</b></div>",
        template_html,
        count=1,
        flags=re.DOTALL,
    )

    template_html = re.sub(
        r"const services = \[[\s\S]*?\];",
        script_services,
        template_html,
        count=1,
    )
    template_html = re.sub(
        r"const hospitals = \[[\s\S]*?;\n\n  function mkHospital[\s\S]*?^  }\n",
        script_hospitals + "\n",
        template_html,
        count=1,
        flags=re.MULTILINE,
    )
    template_html = template_html.replace(
        "Mock data • Fields reflect your parquet columns",
        "Generated from fact.csv",
    )
    template_html = template_html.replace(
        '  selectHospital("h2");\n  showMapCard("h2");',
        "  if(hospitals.length){\n    selectHospital(hospitals[0].id);\n    showMapCard(hospitals[0].id);\n  }",
    )
    return template_html


def output_name_from_template(template_path: Path) -> str:
    name = template_path.name
    if "_mock" in name:
        return name.replace("_mock", "_generated", 1)
    return f"{template_path.stem}_generated{template_path.suffix}"


def main():
    parser = argparse.ArgumentParser(
        description="Generate Unraveled HTML from fact.csv using one or more mock templates."
    )
    parser.add_argument("--csv", default="fact.csv", help="Input CSV path.")
    parser.add_argument(
        "--template",
        default=None,
        help="Single HTML template path to reuse. If omitted, all matching templates in --templates-dir are used.",
    )
    parser.add_argument(
        "--templates-dir",
        default="html_format",
        help="Folder containing mock HTML templates for batch generation.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Generated HTML output path. Used only with --template.",
    )
    parser.add_argument("--services", type=int, default=10, help="Selected services count.")
    parser.add_argument("--hospitals", type=int, default=20, help="Providers to include.")
    args = parser.parse_args()

    csv_path = Path(args.csv)

    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    template_paths = []
    if args.template:
        template_path = Path(args.template)
        if not template_path.exists():
            raise SystemExit(f"Template HTML not found: {template_path}")
        template_paths = [template_path]
    else:
        templates_dir = Path(args.templates_dir)
        if not templates_dir.exists():
            raise SystemExit(f"Templates directory not found: {templates_dir}")
        template_paths = sorted(templates_dir.glob("unraveled_price_services_mock*.html"))
        if not template_paths:
            raise SystemExit(
                f"No templates found in {templates_dir} matching unraveled_price_services_mock*.html"
            )

    if args.output and len(template_paths) != 1:
        raise SystemExit("--output can only be used when a single --template is provided.")

    services, hospitals = build_dataset(csv_path, args.services, args.hospitals)
    if not services:
        raise SystemExit("No services were extracted from CSV.")
    if not hospitals:
        raise SystemExit("No provider rows were extracted from CSV.")

    for template_path in template_paths:
        output_path = Path(args.output) if args.output else Path(output_name_from_template(template_path))
        html = render_html(template_path.read_text(encoding="utf-8"), services, hospitals)
        output_path.write_text(html, encoding="utf-8")
        print(
            f"Generated {output_path} from {template_path} with {len(services)} services and {len(hospitals)} providers."
        )


if __name__ == "__main__":
    main()
