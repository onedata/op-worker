%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Space strategy configuration macros.
%%% @end
%%%-------------------------------------------------------------------

-record(space_strategy_argument, {
    name :: space_strategy:argument_name(),
    type :: space_strategy:argument_type(),
    description :: space_strategy:description()
}).

-record(space_strategy, {
    result_merge_type = return_none :: space_strategy:job_merge_type(),
    name :: space_strategy:name(),
    arguments = [] :: [#space_strategy_argument{}],
    description :: space_strategy:description()
}).

-record(space_strategy_job, {
    strategy_type = ?MODULE :: space_strategy:type(),
    strategy_name :: space_strategy:name(),
    strategy_args :: space_strategy:arguments(),
    data :: space_strategy:job_data()
}).
