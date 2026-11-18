nextflow.enable.dsl = 2

include { MATCH as MATCH_BATCH } from './modules/match'
include { MATCH as MATCH_RECOVERY } from './modules/match'
include { MATCH as MATCH_LONG } from './modules/match'

def hasWork(batch) {
    def descriptor = new groovy.json.JsonSlurper().parseText(batch.text)
    return !descriptor.ids.isEmpty()
}

def needsRecovery(outcomes) {
    return outcomes.any { outcome ->
        def descriptor = new groovy.json.JsonSlurper().parseText(outcome.text)
        !descriptor.unfinished.isEmpty()
    }
}

process EXTRACT {
    input:
    path analysis, stageAs: 'analysis.json'
    output:
    path 'extraction.json'
    script:
    """
    python3 '${params.worker}' extract --params analysis.json --seconds ${task.time.toSeconds() - 120}
    """
}

process PREPARE_MATCH_BATCHES {
    input:
    path analysis, stageAs: 'analysis.json'
    path extraction, stageAs: 'extraction.json'
    output:
    path 'batches/*.json', emit: batches
    path 'expected.json', emit: expected
    script:
    """
    python3 '${params.worker}' prepare --params analysis.json --input extraction.json
    """
}

process PREPARE_RECOVERY {
    input:
    path analysis, stageAs: 'analysis.json'
    path outcomes, stageAs: 'outcomes/outcome??.json'
    output:
    path 'batches/*.json'
    script:
    """
    python3 '${params.worker}' repack --params analysis.json --outcomes ${outcomes}
    """
}

process PREPARE_LONG {
    input:
    path analysis, stageAs: 'analysis.json'
    path outcomes, stageAs: 'outcomes/outcome??.json'
    output:
    path 'batches/*.json'
    script:
    """
    python3 '${params.worker}' repack --params analysis.json --final --outcomes ${outcomes}
    """
}

process PREPARE_RECONCILE {
    input:
    path analysis, stageAs: 'analysis.json'
    path expected, stageAs: 'expected.json'
    path outcomes, stageAs: 'outcomes/outcome??.json'
    output:
    path 'reconcile_batches/*.json', emit: batches
    path 'summary_plan.json', emit: plan
    script:
    """
    python3 '${params.worker}' prepare-reconcile --params analysis.json --input expected.json --outcomes ${outcomes}
    """
}

process RECONCILE {
    input:
    path analysis, stageAs: 'analysis.json'
    tuple val(shard_id), path(batch, stageAs: 'batch.json')
    output:
    tuple val(shard_id), path('batch.json'), path('matches.tar')
    script:
    """
    python3 '${params.worker}' reconcile --params analysis.json --input batch.json --archive
    """
}

process SUMMARIZE_SHARD {
    tag shard_id
    input:
    path analysis, stageAs: 'analysis.json'
    tuple val(shard_id), path(shard, stageAs: 'shard.json'), path(archive, stageAs: 'matches.tar')
    output:
    tuple val(shard_id), path('shard.json'), path('reports.tar'), path('diagnostic_inputs.tar')
    script:
    """
    python3 '${params.worker}' summarize-shard --params analysis.json --input shard.json --archives matches.tar --workers ${task.cpus} --seconds ${task.time.toSeconds() - 120}
    """
}

process EXPORT_DIAGNOSTICS {
    tag shard_id
    input:
    path analysis, stageAs: 'analysis.json'
    tuple val(shard_id), path(shard, stageAs: 'shard.json'), path(archive, stageAs: 'diagnostic_inputs.tar')
    output:
    tuple val(shard_id), path('diagnostics.tar')
    script:
    """
    python3 '${params.worker}' export-diagnostics --params analysis.json --input shard.json --archives diagnostic_inputs.tar --workers ${task.cpus} --seconds ${task.time.toSeconds() - 120}
    """
}

process FINALIZE_REPORTS {
    input:
    path analysis, stageAs: 'analysis.json'
    path plan, stageAs: 'summary_plan.json'
    path archives, stageAs: 'reports/shard??.tar'
    output:
    path 'final_reports.tar'
    script:
    """
    python3 '${params.worker}' finalize-reports --params analysis.json --input summary_plan.json --archives ${archives} --seconds ${task.time.toSeconds() - 120}
    """
}

process PUBLISH_RESULTS {
    publishDir "${params.state_uri}/published", mode: 'copy', overwrite: true
    input:
    path analysis, stageAs: 'analysis.json'
    path plan, stageAs: 'summary_plan.json'
    path report, stageAs: 'final_reports.tar'
    path diagnostics, stageAs: 'diagnostics/shard??.tar'
    output:
    path 'results.json'
    script:
    """
    python3 '${params.worker}' publish-summary --params analysis.json --input summary_plan.json --report final_reports.tar --archives ${diagnostics}
    """
}

workflow {
    if (!params.analysis_file || !params.state_uri) {
        error 'analysis_file and state_uri are required'
    }
    def target = file("${params.state_uri}/session.json")
    target.parent.mkdirs()
    target.text = groovy.json.JsonOutput.toJson([
        session_id: workflow.sessionId.toString(),
        workflow_version: '1.2.0'
    ])
    analysis = Channel.value(file(params.analysis_file, checkIfExists: true))
    EXTRACT(analysis)
    PREPARE_MATCH_BATCHES(analysis, EXTRACT.out)
    MATCH_BATCH(analysis, PREPARE_MATCH_BATCHES.out.batches.flatten().filter { batch -> hasWork(batch) })
    PREPARE_RECOVERY(analysis, MATCH_BATCH.out.toList().filter { outcomes -> needsRecovery(outcomes) })
    MATCH_RECOVERY(analysis, PREPARE_RECOVERY.out.flatten().filter { batch -> hasWork(batch) })
    PREPARE_LONG(analysis, MATCH_RECOVERY.out.toList().filter { outcomes -> needsRecovery(outcomes) })
    MATCH_LONG(analysis, PREPARE_LONG.out.flatten().filter { batch -> hasWork(batch) })
    PREPARE_RECONCILE(analysis, PREPARE_MATCH_BATCHES.out.expected, MATCH_LONG.out.toList())
    RECONCILE(analysis, PREPARE_RECONCILE.out.batches.flatten().map { batch -> tuple(batch.baseName, batch) })
    SUMMARIZE_SHARD(analysis, RECONCILE.out)
    EXPORT_DIAGNOSTICS(analysis, SUMMARIZE_SHARD.out.map { id, shard, reports, diagnostics -> tuple(id, shard, diagnostics) })
    FINALIZE_REPORTS(analysis, PREPARE_RECONCILE.out.plan, SUMMARIZE_SHARD.out.map { id, shard, reports, diagnostics -> reports }.toList())
    PUBLISH_RESULTS(analysis, PREPARE_RECONCILE.out.plan, FINALIZE_REPORTS.out, EXPORT_DIAGNOSTICS.out.map { id, diagnostics -> diagnostics }.toList())
}
