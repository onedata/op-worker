%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module produces an optional, very verbose trace of every operation
%%% performed by the provider while an archive is being created. 
%%%
%%% The trace is disabled by default and is enabled with the
%%% `archivisation_verbose_logs_enabled` app.config variable.
%%%
%%% Every instrumented operation is reported twice - `report_started/1,2` is
%%% called before it begins and returns a context describing it, which is then
%%% passed to `report_finished/1,2` after the operation returns, to report it
%%% along with the measured duration. One-shot occurrences that have no duration
%%% (e.g. state transitions) are reported with `report_event/1,2`.
%%%
%%% Archivisation is a highly concurrent process and entries of different
%%% jobs are interleaved - they are told apart by the pid, which the log 
%%% prints for every line.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_logger).
-author("Michal Stanisz").

-include_lib("ctool/include/logging.hrl").

%% API
-export([report_started/1, report_started/2]).
-export([report_finished/1, report_finished/2]).
-export([report_event/1, report_event/2]).

-record(archivisation_logger_ctx, {
    description :: description(),
    details :: details(),
    stopwatch :: stopwatch:instance()
}).

-type description() :: string().
% details are built at the call site with the ?autoformat/?autoformat_with_msg macros
-type details() :: string() | #autoformat_spec{}.
-type ctx() :: #archivisation_logger_ctx{}.

-export_type([description/0, details/0, ctx/0]).

-define(ARE_VERBOSE_LOGS_ENABLED, op_worker:get_env(archivisation_verbose_logs_enabled, false)).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec report_started(description()) -> ctx().
report_started(Description) ->
    report_started(Description, "").


-spec report_started(description(), details()) -> ctx().
report_started(Description, Details) ->
    log("Archivisation operation started: ~ts", [Description], Details),
    #archivisation_logger_ctx{
        description = Description,
        details = Details,
        stopwatch = stopwatch:start()
    }.


-spec report_finished(ctx()) -> ok.
report_finished(#archivisation_logger_ctx{details = Details} = ArchivisationLoggerCtx) ->
    report_finished(ArchivisationLoggerCtx, Details).


%%--------------------------------------------------------------------
%% @doc
%% Reports the end of an operation with details other than those reported
%% when it started - to be used when the outcome of the operation is worth
%% reporting alongside its duration.
%% @end
%%--------------------------------------------------------------------
-spec report_finished(ctx(), details()) -> ok.
report_finished(#archivisation_logger_ctx{description = Description, stopwatch = Stopwatch}, Details) ->
    log("Archivisation operation finished (took ~B ms): ~ts",
        [stopwatch:read_millis(Stopwatch), Description], Details).


-spec report_event(description()) -> ok.
report_event(Description) ->
    report_event(Description, "").


-spec report_event(description(), details()) -> ok.
report_event(Description, Details) ->
    log("Archivisation event: ~ts", [Description], Details).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec log(string(), [term()], details()) -> ok.
log(Format, Args, Details) ->
    case ?ARE_VERBOSE_LOGS_ENABLED of
        true ->
            ?info(Format ++ "~ts", Args ++ [onedata_logger:format_generic_log(Details, [])]);
        false ->
            ok
    end.
