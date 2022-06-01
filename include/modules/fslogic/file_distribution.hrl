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


-record(provider_dir_distribution, {
    logical_size :: undefined | file_meta:size(),
    physical_size_per_storage = #{} :: #{od_storage:id() => undefined | non_neg_integer()}
}).

-record(dir_distribution, {
    distribution_per_provider = #{} :: #{
        od_provider:id() => file_distribution:provider_dir_distribution() | errors:error()
    }
}).

-record(reg_distribution, {
    logical_size = 0 :: file_meta:size(),
    blocks_per_provider = #{} :: #{od_provider:id() => #{od_storage:id() => fslogic_blocks:blocks()}}
}).

-record(symlink_distribution, {}).

-record(file_distribution_get_request, {}).

-record(file_distribution_get_result, {
    distribution ::
        file_distribution:dir_distribution() |
        file_distribution:symlink_distribution() |
        file_distribution:reg_distribution()
}).


-endif.
