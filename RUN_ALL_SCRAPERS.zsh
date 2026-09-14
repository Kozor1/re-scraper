#!/bin/zsh
# Script to re-run all property scrapers
# Run this from the re-scraper directory

echo "========================================"
echo "Re-running ALL property scrapers"
echo "========================================"
echo ""

# Create logs directory if it doesn't exist
mkdir -p logs

# Function to run a scraper
run_scraper() {
    local name=$1
    local script=$2
    echo "Starting $name scraper..."
    python3 scrapers/$script --fresh > logs/${name}_$(date +%Y%m%d_%H%M%S).log 2>&1 &
    echo "  PID: $!"
}

# Run all scrapers in background
echo "Starting all scrapers in background..."
echo ""

run_scraper "bmc" "bmc_full_scrape.py"
run_scraper "ta" "ta_full_scrape.py"
run_scraper "rr" "rr_full_scrape.py"
run_scraper "ft" "ft_full_scrape.py"
run_scraper "abc" "abc_full_scrape.py"
run_scraper "amd" "amd_full_scrape.py"
run_scraper "hn" "hn_full_scrape.py"
run_scraper "dl" "dl_full_scrape.py"
run_scraper "tm" "tm_full_scrape.py"
run_scraper "le" "le_full_scrape.py"
run_scraper "mc" "mc_full_scrape.py"
run_scraper "ee" "ee_full_scrape.py"
run_scraper "ag" "ag_full_scrape.py"
run_scraper "ups" "ups_full_scrape.py"
run_scraper "sb" "sb_full_scrape.py"
run_scraper "tr" "tr_full_scrape.py"
run_scraper "cps" "cps_full_scrape.py"
run_scraper "hc" "hc_full_scrape.py"
run_scraper "jm" "jm_full_scrape.py"
run_scraper "ma" "ma_full_scrape.py"
run_scraper "hg" "hg_full_scrape.py"
run_scraper "pr" "pr_full_scrape.py"
run_scraper "pp" "pp_full_scrape.py"
run_scraper "ipe" "ipe_full_scrape.py"
run_scraper "pe" "pe_full_scrape.py"
run_scraper "bt" "bt_full_scrape.py"
run_scraper "ag2" "ag2_full_scrape.py"
run_scraper "mmc" "mmc_full_scrape.py"
run_scraper "nest" "nest_full_scrape.py"

echo ""
echo "========================================"
echo "All scrapers started!"
echo "========================================"
echo ""
echo "To check progress:"
echo "  tail -f logs/*.log"
echo ""
echo "To check running processes:"
echo "  ps aux | grep '_full_scrape'"
echo ""
echo "To kill all scrapers:"
echo "  pkill -f '_full_scrape'"
echo ""
