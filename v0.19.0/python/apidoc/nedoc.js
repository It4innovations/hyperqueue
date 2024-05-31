$(function() {
    var NEDOC_MODULES = JSON.parse('[["Client", "hyperqueue.client", true], ["ClientConnection", "hyperqueue.ffi.client", true], ["CloudWrapper", "hyperqueue.task.function.wrapper", true], ["Cluster", "hyperqueue.ffi.cluster", true], ["ExternalProgram", "hyperqueue.task.program", true], ["FailedJobsException", "hyperqueue.client", true], ["FailedTaskContext", "hyperqueue.ffi.client", true], ["HqClientContext", "hyperqueue.ffi.client", true], ["HqClusterContext", "hyperqueue.ffi.cluster", true], ["Job", "hyperqueue.job", true], ["JobDescription", "hyperqueue.ffi.protocol", true], ["LocalCluster", "hyperqueue.cluster", true], ["MissingPackageException", "hyperqueue.utils.package", true], ["Output", "hyperqueue.output", true], ["PythonEnv", "hyperqueue.task.function", true], ["PythonFunction", "hyperqueue.task.function", true], ["ResourceRequest", "hyperqueue.ffi.protocol", true], ["StdioDef", "hyperqueue.output", true], ["SubmittedJob", "hyperqueue.job", true], ["Task", "hyperqueue.task.task", true], ["TaskDescription", "hyperqueue.ffi.protocol", true], ["ValidationException", "hyperqueue.validation", true], ["WorkerConfig", "hyperqueue.cluster", true], ["__call__", "hyperqueue.task.function.wrapper.CloudWrapper", false], ["__enter__", "hyperqueue.cluster.LocalCluster", false], ["__exit__", "hyperqueue.cluster.LocalCluster", false], ["__getitem__", "hyperqueue.task.program.ExternalProgram", false], ["__init__", "hyperqueue.client.Client", false], ["__init__", "hyperqueue.client.FailedJobsException", false], ["__init__", "hyperqueue.cluster.LocalCluster", false], ["__init__", "hyperqueue.ffi.client.ClientConnection", false], ["__init__", "hyperqueue.ffi.cluster.Cluster", false], ["__init__", "hyperqueue.ffi.protocol.ResourceRequest", false], ["__init__", "hyperqueue.job.Job", false], ["__init__", "hyperqueue.output.Output", false], ["__init__", "hyperqueue.task.function.PythonEnv", false], ["__init__", "hyperqueue.task.function.PythonFunction", false], ["__init__", "hyperqueue.task.function.wrapper.CloudWrapper", false], ["__init__", "hyperqueue.task.program.ExternalProgram", false], ["__init__", "hyperqueue.task.task.Task", false], ["__init__", "hyperqueue.utils.package.MissingPackageException", false], ["__reduce__", "hyperqueue.task.function.wrapper.CloudWrapper", false], ["__repr__", "hyperqueue.ffi.protocol.ResourceRequest", false], ["__repr__", "hyperqueue.task.function.PythonFunction", false], ["__repr__", "hyperqueue.task.function.wrapper.CloudWrapper", false], ["__repr__", "hyperqueue.task.program.ExternalProgram", false], ["__str__", "hyperqueue.client.FailedJobsException", false], ["__str__", "hyperqueue.utils.package.MissingPackageException", false], ["_add_task", "hyperqueue.job.Job", false], ["_build", "hyperqueue.job.Job", false], ["_build", "hyperqueue.task.function.PythonFunction", false], ["_build", "hyperqueue.task.program.ExternalProgram", false], ["_build", "hyperqueue.task.task.Task", false], ["_get_pickled_fn", "hyperqueue.task.function.wrapper.CloudWrapper", false], ["_make_ffi_requests", "hyperqueue.task.task", false], ["add_worker", "hyperqueue.ffi.cluster.Cluster", false], ["build_stdio", "hyperqueue.task.task", false], ["client", "hyperqueue", true], ["client", "hyperqueue.cluster.LocalCluster", false], ["client", "hyperqueue.ffi", true], ["cloud_wrap", "hyperqueue.task.function", false], ["cluster", "hyperqueue", true], ["cluster", "hyperqueue.ffi", true], ["common", "hyperqueue", true], ["create_progress_callback", "hyperqueue.client", false], ["default_output", "hyperqueue.output", false], ["default_stderr", "hyperqueue.output", false], ["default_stdout", "hyperqueue.output", false], ["ffi", "hyperqueue", true], ["forget", "hyperqueue.client.Client", false], ["forget_job", "hyperqueue.ffi.client.ClientConnection", false], ["from_path", "hyperqueue.output.StdioDef", false], ["function", "hyperqueue.job.Job", false], ["function", "hyperqueue.task", true], ["gather_outputs", "hyperqueue.output", false], ["generate_name", "hyperqueue.output", false], ["generate_random_name", "hyperqueue.output", false], ["generate_task_name", "hyperqueue.task.function", false], ["get_failed_tasks", "hyperqueue.client.Client", false], ["get_failed_tasks", "hyperqueue.ffi.client.ClientConnection", false], ["get_job_id", "hyperqueue.job", false], ["get_logging_level", "hyperqueue.task.function", false], ["get_task_outputs", "hyperqueue.task.program", false], ["get_version", "hyperqueue.ffi", false], ["is_generator_function", "hyperqueue.task.function.wrapper.CloudWrapper", false], ["job", "hyperqueue", true], ["label", "hyperqueue.task.task.Task", false], ["materialize_outputs", "hyperqueue.output", false], ["merge_envs", "hyperqueue.job", false], ["output", "hyperqueue", true], ["package", "hyperqueue.utils", true], ["pluralize", "hyperqueue.utils.string", false], ["program", "hyperqueue.job.Job", false], ["program", "hyperqueue.task", true], ["protocol", "hyperqueue.ffi", true], ["purge_cache", "hyperqueue.task.function", false], ["remove_if_finished", "hyperqueue.output.StdioDef", false], ["server_dir", "hyperqueue.ffi.cluster.Cluster", false], ["start_worker", "hyperqueue.cluster.LocalCluster", false], ["stop", "hyperqueue.cluster.LocalCluster", false], ["stop", "hyperqueue.ffi.cluster.Cluster", false], ["stop_server", "hyperqueue.ffi.client.ClientConnection", false], ["string", "hyperqueue.utils", true], ["submit", "hyperqueue.client.Client", false], ["submit_job", "hyperqueue.ffi.client.ClientConnection", false], ["task", "hyperqueue", true], ["task", "hyperqueue.task", true], ["task_by_id", "hyperqueue.job.Job", false], ["task_label", "hyperqueue.client.FailedJobsException", false], ["task_main", "hyperqueue.task.function", false], ["to_arg_list", "hyperqueue.task.program", false], ["utils", "hyperqueue", true], ["validate_args", "hyperqueue.validation", false], ["validation", "hyperqueue", true], ["visualization", "hyperqueue", true], ["visualize_job", "hyperqueue.visualization", false], ["wait_for_jobs", "hyperqueue.client.Client", false], ["wait_for_jobs", "hyperqueue.ffi.client.ClientConnection", false], ["wrapper", "hyperqueue.task.function", true]]');
    $("#search").autocomplete({
    source: NEDOC_MODULES.map(function(i) { return { label: i[0], desc: i[1], fulllink: i[2] }; }),
    select: function(event, ui) {
        if (ui.fulllink) {
            window.location.href = ui.item.desc + "." + ui.item.label + ".html";
        } else {
            window.location.href = ui.item.desc + ".html#" + ui.item.label;
        }
    },
    }).autocomplete("instance")._renderItem = function(ul, item) {
        return $("<li>")
            .append("<div><b>" + item.label + "</b><br>" + item.desc + "</div>")
            .appendTo(ul);
    };

    $(".fexpand").on("click", function(event) {
        event.preventDefault();
        var elem = $(this);
        var parent = elem.closest(".fn");
        var fdetail = parent.children(".fdetail");
        fdetail.toggle(200, "swing", function() {
            var is_visible = fdetail.is(':visible');
            var toggle = elem.parent().children(".ftoggle");
            var toggle2 = elem.parent().children(".ftoggle-empty");
            if (is_visible) {
                toggle.html("&#9660;");
                toggle2.html("&#9661;");
            } else {
                toggle.html("&#9654;");
                toggle2.html("&#9655;");
            }
        });
    })

    if(window.location.hash) {
        var name = window.location.hash.slice(3); // remove #f_ prefix
        var elem = $("#fn_" + name);
        elem.toggle(0);
        elem.parent().parent().css("backgroundColor", "#e9f6ff");
    }
});
