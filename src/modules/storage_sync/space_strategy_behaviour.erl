%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Behaviour of a space strategy type.
%%% @end
%%%-------------------------------------------------------------------
-module(space_strategy_behaviour).
-author("Rafal Slota").


%%%===================================================================
%%% Callback definitions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns descriptions of available strategies for this strategy type.
%% @end
%%--------------------------------------------------------------------
-callback available_strategies() -> [space_strategy:definition()].

%%--------------------------------------------------------------------
%% @doc
%% Initializes strategy job. Returns list of init jobs, all of which will be passed to strategy_handle_job/1.
%% @end
%%--------------------------------------------------------------------
-callback strategy_init_jobs(space_strategy:name(), space_strategy:arguments(), space_strategy:job_data()) ->
    [space_strategy:job()].

%%--------------------------------------------------------------------
%% @doc
%% Handles given strategy job. This function shall return result of this handle and further child
%% jobs to process afterwards.
%% @end
%%--------------------------------------------------------------------
-callback strategy_handle_job(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.

%%--------------------------------------------------------------------
%% @doc
%% For given 'children' jobs and its results, this function has to return merged job result that will
%% be passed as "ChildrenResult" in strategy_merge_result/3.
%% @end
%%--------------------------------------------------------------------
-callback strategy_merge_result(ChildrenJobs :: [space_strategy:job()],
    ChildrenResults :: [space_strategy:job_result()]) ->
    space_strategy:job_result().

%%--------------------------------------------------------------------
%% @doc
%% Merges job result with already merged children job results.
%% @end
%%--------------------------------------------------------------------
-callback strategy_merge_result(space_strategy:job(), LocalResult :: space_strategy:job_result(),
    ChildrenResult :: space_strategy:job_result()) ->
    space_strategy:job_result().

