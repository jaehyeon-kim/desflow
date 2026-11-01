"""Docs tests that a break of 1.0.0 makes fail, skipped until Batch 13 handles their pages.

Batches 1 to 12 edit no hand-written page, so a test that fails because the page is
wrong is skipped here with its reason. docs-changes.md in the 1.0.0 plan lists each
row. Batch 13 deletes an entry when it fixes the page.
"""

T1 = "Batch 1 (T1): strict defaults to true when factor is above 0, so this page's paced run raises when it falls behind; Batch 13 handles the page"
T14 = "Batch 1 (T14): strict: true at factor 0 now raises once the wall clock has moved, so this page's snippet fails; Batch 13 drops strict: true from the snippet or sets a factor above 0"
T2 = "Batch 1 (T2): on_unhandled_failure defaults to stop, so this page's model stops at its failure; Batch 13 handles the page"
T3 = "Batch 1 (T3): signals are one-shot by default, so this page's model fires a signal twice; Batch 13 adds once: false to the page"
T6 = "Batch 1 (T6): records keep sim_ts unrounded, so this page shows or asserts rounded times; Batch 13 handles the page"
T7 = "Batch 1 (T7, MF2): numbers keep the type they are written in, so this page shows 38.0 where the code now gives 38; Batch 13 regenerates the block"
I5 = "Batch 2 (I5): a patience that ends at the instant its unit is granted reneges, so this page's tie check reads reneged where it asserts served; Batch 13 rewrites the page and the check"
NEW_KEY = "Batch 1: the key is new and no hand-written page shows it yet; Batch 13 adds the example"

SKIPPED = {
    "test_docs_command_line.py::test_every_command_on_the_page_runs": T6,
    "test_docs_example_answers.py::test_backfill_live_history_rows_and_first_live_event": T6,
    "test_docs_guides.py::test_analyse_the_event_log_waits_and_throughput": T6,
    "test_docs_guides.py::test_how_to_check_scripts_run_as_the_page_builds_them[how-to/modelling/wait-for-one-or-all.md-wait_for_one_or_all-wait_for_one_or_all.py-from desflow import Entity, SimulationContext-False]": T2,
    "test_docs_guides.py::test_how_to_check_scripts_run_as_the_page_builds_them[how-to/modelling/reneging-and-balking.md-reneging_and_balking-check_reneging.py-import_line1-False]": I5,
    "test_docs_guides.py::test_reneging_and_balking_ties_and_balance": I5,
    "test_docs_guides.py::test_wait_for_one_or_all_checks_run_from_saved_files": T2,
    "test_docs_guides.py::test_wait_for_one_or_all_example_packs_and_refunds": T3,
    "test_docs_guides.py::test_wait_for_one_or_all_first_ties_and_failure": T2,
    "test_docs_page_snippets.py::test_failures_print_what_the_page_says": T2,
    "test_docs_page_snippets.py::test_page_snippet_runs[failures.py]": T2,
    "test_docs_record_blocks.py::test_block_is_up_to_date[backfill-live-sample]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[builder-first]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[cli-sample]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[courier-yaml]": T3,
    "test_docs_record_blocks.py::test_block_is_up_to_date[end-yaml]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[first-sample]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[first-trace]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[gift-flow]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[holder-readings]": T7,
    "test_docs_record_blocks.py::test_block_is_up_to_date[hybrid-events]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[iceberg-sample]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[kafka-sample]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[local-records]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[one-order-flow]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[parquet-sample]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[patience-reneged]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[postgres-sample]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[record-examples]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[redis-sample]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[related-records-sample]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[stock-out]": T7,
    "test_docs_record_blocks.py::test_block_is_up_to_date[task-events]": T6,
    "test_docs_record_blocks.py::test_block_is_up_to_date[twelfth-order]": T6,
    "test_docs_snippets.py::test_snippet_builds_and_runs[environment.yaml]": T14,
    "test_docs_tutorials.py::test_first_simulation_same_records_and_first_wait": T6,
    "test_docs_match_examples.py::test_documented_source_matches_the_script[docs/user-guide/tasks-and-processes/signals-and-waiting.md]": T3,
    "test_docs_tutorials.py::test_process_interaction_courier_shares[builder]": T3,
    "test_docs_tutorials.py::test_process_interaction_courier_shares[low-level]": T3,
    "test_docs_tutorials.py::test_process_interaction_courier_shares[yaml]": T3,
    "test_docs_tutorials.py::test_randomness_and_seeds_same_wait_on_every_run_and_after_returns": T6,
    "test_docs_tutorials.py::test_randomness_and_seeds_twelfth_order[builder]": T6,
    "test_docs_tutorials.py::test_randomness_and_seeds_twelfth_order[low-level]": T6,
    "test_docs_tutorials.py::test_randomness_and_seeds_twelfth_order[yaml]": T6,
    "test_docs_tutorials.py::test_streaming_out_records[builder]": T6,
    "test_docs_tutorials.py::test_streaming_out_records[low-level]": T6,
    "test_docs_tutorials.py::test_streaming_out_records[yaml]": T6,
    "test_explain_sweep.py::test_verify_finds_no_difference[docs/snippets/yaml/process_interaction.yaml]": T3,
    "test_feature_coverage.py::test_every_blueprint_key_has_an_example_a_reference_and_a_skill_entry[Run.segments]": NEW_KEY,
    "test_feature_coverage.py::test_every_blueprint_key_has_an_example_a_reference_and_a_skill_entry[Run.steps]": NEW_KEY,
    "test_feature_coverage.py::test_every_blueprint_key_has_an_example_a_reference_and_a_skill_entry[Run.sync]": NEW_KEY,
    "test_feature_coverage.py::test_every_blueprint_key_has_an_example_a_reference_and_a_skill_entry[RunSegment.look]": NEW_KEY,
    "test_feature_coverage.py::test_every_blueprint_key_has_an_example_a_reference_and_a_skill_entry[RunSegment.stop]": NEW_KEY,
    "test_feature_coverage.py::test_every_blueprint_key_has_an_example_a_reference_and_a_skill_entry[RunSegment.sync]": NEW_KEY,
    "test_feature_coverage.py::test_every_blueprint_key_has_an_example_a_reference_and_a_skill_entry[SegmentEvery.every]": NEW_KEY,
}
