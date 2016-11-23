%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @todo: write me!
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

-record(space_strategy_argument, {
    name :: space_strategy:argument_name(),
    type :: space_strategy:argument_type(),
    description :: space_strategy:description()
}).

-record(space_strategy, {
    name :: space_strategy:name(),
    arguments = [] :: [#space_strategy_argument{}],
    description :: space_strategy:description()
}).

-record(space_strategy_job, {
    strategy_name :: space_strategy:name(),
    strategy_args :: space_strategy:arguments(),
    data :: space_strategy:job_data()
}).
