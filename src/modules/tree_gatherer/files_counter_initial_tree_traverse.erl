%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% % NOTE: nie sprawdzac tego modulu - czeka na dyskusje po standup ak zapewnic ze plik nie zostanie
%%% zainicjalizowany 2 razy.
%%% @end
%%%-------------------------------------------------------------------
-module(files_counter_initial_tree_traverse).
-author("Michal Wrzeszcz").


-behavior(traverse_behaviour).


-include("tree_traverse.hrl").


%% API
-export([init_for_all/0]).

%% Traverse callbacks
-export([do_master_job/2, do_slave_job/2, update_job_progress/5, get_job/1]).


%%%===================================================================
%%% API
%%%===================================================================

init_for_all() ->
    ok.


%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

do_master_job(Job, TaskID) ->
    tree_traverse:do_master_job(Job, TaskID).


do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx
}, _TaskID) ->
    Guid = file_ctx:get_logical_guid_const(FileCtx),
    {Size, _} = file_ctx:get_file_size(FileCtx),
    files_counter:increment_count(Guid),
    files_counter:update_size(Guid, Size),
    ok.


update_job_progress(ID, Job, Pool, TaskID, Status) ->
    tree_traverse:update_job_progress(ID, Job, Pool, TaskID, Status, ?MODULE).


get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).