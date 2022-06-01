%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% File distribution related record definitions.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(FILE_DISTRIBUTION_HRL).
-define(FILE_DISTRIBUTION_HRL, 1).


-record(dir_distribution, {
    logical_size = 0 :: non_neg_integer(),
    physical_size_per_storage = #{} :: #{od_storage:id() => non_neg_integer()}
}).

-record(reg_distribution, {
    logical_size = 0 :: non_neg_integer(),
    blocks_per_storage = #{} :: #{od_storage:id() => fslogic_blocks:blocks()}
}).

-record(file_distribution_get_request, {}).

-record(file_distribution_get_result, {
    distribution ::
        file_distribution:dir_distribution() |
        file_distribution:reg_distribution()
}).


-endif.
