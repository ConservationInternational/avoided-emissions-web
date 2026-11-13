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
    python3 '${params.worker}' repack --params analysis.json --outcomes outcomes/*.json
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
    python3 '${params.worker}' repack --params analysis.json --final --outcomes outcomes/*.json
    """
}

process RECONCILE {
    input:
    path analysis, stageAs: 'analysis.json'
    path expected, stageAs: 'expected.json'
    path outcomes, stageAs: 'outcomes/outcome??.json'
    output:
    path 'reconciled.json', emit: descriptor
    path 'matches', emit: matches
    script:
    """
    python3 '${params.worker}' reconcile --params analysis.json --input expected.json --outcomes outcomes/*.json
    """
}

process SUMMARIZE {
    publishDir "${params.state_uri}/published", mode: 'copy', overwrite: true

    input:
    path analysis, stageAs: 'analysis.json'
    path reconciled, stageAs: 'reconciled.json'
    path matches, stageAs: 'matches'
    output:
    path 'results.json'
    script:
    """
    python3 '${params.worker}' summarize --params analysis.json --input reconciled.json --matches matches --seconds ${task.time.toSeconds() - 120}
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
        workflow_version: '1.0.0'
    ])
    analysis = Channel.value(file(params.analysis_file, checkIfExists: true))
    EXTRACT(analysis)
    PREPARE_MATCH_BATCHES(analysis, EXTRACT.out)
    MATCH_BATCH(analysis, PREPARE_MATCH_BATCHES.out.batches.flatten().filter { batch -> hasWork(batch) })
    PREPARE_RECOVERY(analysis, MATCH_BATCH.out.toList().filter { outcomes -> needsRecovery(outcomes) })
    MATCH_RECOVERY(analysis, PREPARE_RECOVERY.out.flatten().filter { batch -> hasWork(batch) })
    PREPARE_LONG(analysis, MATCH_RECOVERY.out.toList().filter { outcomes -> needsRecovery(outcomes) })
    MATCH_LONG(analysis, PREPARE_LONG.out.flatten().filter { batch -> hasWork(batch) })
    RECONCILE(analysis, PREPARE_MATCH_BATCHES.out.expected, MATCH_LONG.out.toList())
    SUMMARIZE(analysis, RECONCILE.out.descriptor, RECONCILE.out.matches)
}
