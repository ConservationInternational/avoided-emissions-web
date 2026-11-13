process MATCH {
    tag { batch.baseName }

    input:
    path analysis, stageAs: 'analysis.json'
    path batch, stageAs: 'batch.json'

    output:
    path 'outcome.json'

    script:
    def seconds = Math.max(1, task.time.toSeconds() - 120)
    """
    python3 '${params.worker}' match --params analysis.json --input batch.json --seconds ${seconds}
    """
}
