# Urban Transportation and Air Quality: A Visual Analytics Study Using NYC TLC Trip Data and OpenAQ

**Project Proposal for DDA3003 Visual Analytics**  
Spring 2026 — The Chinese University of Hong Kong, Shenzhen

---

## 1. What are you trying to do?

**Jargon-free description.** This project explores how everyday urban transportation activities influence air quality in New York City. By integrating large-scale taxi and ride-hailing trip data with air pollution measurements, the project aims to help users visually and interactively understand when, where, and under what conditions transportation intensity is associated with changes in air quality.

**Formal problem definition.** Given transportation demand data T(t, l) derived from NYC TLC trip records and air quality observations A(t, l, p) from OpenAQ (where p denotes pollutant type), the goal is to analyze spatiotemporal relationships between transportation activity and pollutant concentrations using aggregation, correlation analysis, and temporal lag analysis.

---

## 2. How is it done today? What are the limits of current practice?

Existing studies mainly rely on statistical regression, simulation-based traffic models, or satellite observations to analyze transportation-related air pollution. These approaches often face limitations including limited interactivity, difficulty handling large-scale trip-level data, and weak integration between analytical results and human-centered visual exploration.

---

## 3. What is new in your approach? Why will it be successful?

This project emphasizes visual analytics rather than purely statistical modeling. The main novelties include:

- Integration of large-scale NYC TLC trip data with OpenAQ air quality measurements;
- Multi-scale spatial (taxi zone, borough) and temporal (hourly to monthly) exploration;
- Interactive examination of correlations, trends, and temporal lag effects.

This approach bridges complex urban datasets and actionable insights that static analyses cannot effectively support.

---

## 4. Who cares?

Urban planners, environmental agencies, public health researchers, and policy makers benefit from understanding transportation-induced air pollution. The general public also gains from transparent, data-driven tools that support sustainable transportation awareness and decision-making.

---

## 5. If successful, what difference will it make and how do you measure it?

If successful, the project will:

- Identify spatiotemporal hotspots where transportation intensity aligns with poor air quality;
- Support evidence-based congestion pricing and green mobility policies.

Evaluation criteria include quantitative correlation results, system scalability, and qualitative user feedback on usability and insight discovery.