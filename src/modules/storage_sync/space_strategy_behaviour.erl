%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @todo: write me!
%%% @end
%%%-------------------------------------------------------------------
-module(space_strategy_behaviour).
-author("Rafal Slota").


%%%===================================================================
%%% Callback definitions
%%%===================================================================


-callback available_strategies() -> [space_strategy:definition()].

-callback strategy_init_jobs(space_strategy:name(), space_strategy:arguments(), space_strategy:job_data()) -> [space_strategy:job()].

-callback strategy_handle_job(space_strategy:job()) -> {space_strategy:job_result(), [space_strategy:job()]}.

%%-spec available_strategies() -> [space_strategy:definition()].
%%available_strategies() ->
%%    [].
%%
%%
%%-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(), space_strategy:job_data()) ->
%%    [space_strategy:job()].
%%strategy_init_jobs(StrategyName, StartegyArgs) ->
%%    [].
%%
%%
%%-spec strategy_handle_job(space_strategy:job()) -> {space_strategy:job_result(), [space_strategy:job()]}.
%%strategy_handle_job(_) ->
%%    {undefined, []}.