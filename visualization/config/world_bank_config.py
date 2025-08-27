"""
World Bank API Configuration
"""

# G20 countries (ISO Alpha-2 codes from World Bank API)
G20_COUNTRIES = [
    "AR", "AU", "BR", "CA", "CN", "FR", "DE", "IN", "ID", "IT",
    "JP", "MX", "RU", "SA", "ZA", "KR", "TR", "GB", "US", "EU"
]

# World Bank Indicators relevant for air pollution context
INDICATORS = [
    # Health impacts
    "SP.DYN.LE00.IN",  # Life expectancy at birth
    "SH.DYN.MORT",     # Under-5 mortality
    "SP.DYN.AMRT",     # Adult mortality
    "SH.MED.BEDS.ZS",  # Hospital beds
    "SH.MED.PHYS.ZS",  # Physicians

    # Environmental drivers
    "EN.ATM.CO2E.PC",  # CO2 per capita
    "EN.ATM.METH.KT.CE",  # Methane emissions
    "EN.ATM.NOXE.KT.CE",  # Nitrous oxide emissions
    "EG.USE.PCAP.KG.OE",  # Energy use per capita
    "EG.FEC.RNEW.ZS",     # Renewable energy share
    "EG.USE.COMM.FO.ZS",  # Fossil fuel share

    # Socioeconomic context
    "NY.GDP.PCAP.CD",     # GDP per capita
    "SP.URB.TOTL.IN.ZS",  # Urban population %
    "EG.ELC.ACCS.ZS",     # Access to electricity
    "EG.CFT.ACCS.ZS",     # Access to clean cooking fuels

    # Vulnerability & exposure
    "EN.POP.DNST",        # Population density
    "EN.POP.SLUM.UR.ZS"   # Slum population %
]

# Indicator descriptions for better visualization
INDICATOR_DESCRIPTIONS = {
    "SP.DYN.LE00.IN": "Life expectancy at birth (years)",
    "SH.DYN.MORT": "Under-5 mortality rate (per 1,000 live births)",
    "SP.DYN.AMRT": "Adult mortality rate (per 1,000 adults)",
    "SH.MED.BEDS.ZS": "Hospital beds (per 1,000 people)",
    "SH.MED.PHYS.ZS": "Physicians (per 1,000 people)",
    "EN.ATM.CO2E.PC": "CO2 emissions per capita (metric tons)",
    "EN.ATM.METH.KT.CE": "Methane emissions (kt of CO2 equivalent)",
    "EN.ATM.NOXE.KT.CE": "Nitrous oxide emissions (kt of CO2 equivalent)",
    "EG.USE.PCAP.KG.OE": "Energy use per capita (kg of oil equivalent)",
    "EG.FEC.RNEW.ZS": "Renewable energy consumption (% of total)",
    "EG.USE.COMM.FO.ZS": "Fossil fuel energy consumption (% of total)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    "EG.ELC.ACCS.ZS": "Access to electricity (% of population)",
    "EG.CFT.ACCS.ZS": "Access to clean fuels for cooking (% of population)",
    "EN.POP.DNST": "Population density (people per sq. km)",
    "EN.POP.SLUM.UR.ZS": "Population living in slums (% of urban population)"
}

# Time period
START_YEAR = 2020
END_YEAR = 2025

# API base URL
WORLD_BANK_API_BASE = "https://api.worldbank.org/v2" 