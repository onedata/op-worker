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


-record(storage_dir_size, {
    storage_id:: storage:id(),
    % `undefined` physical size means that file was not yet created on this storage.
    physical_size :: undefined | non_neg_integer()
}).

-record(provider_dir_distribution, {
    logical_size :: undefined | file_meta:size(),
    physical_size_per_storage = [] :: [file_distribution:storage_dir_size()]
}).

-record(dir_distribution, {
    distribution_per_provider = #{} :: #{
        od_provider:id() => file_distribution:provider_dir_distribution() | errors:error()
    }
}).


% NOTE: translated to protobuf
-record(storage_reg_distribution, {
    storage_id :: storage:id(),
    blocks :: fslogic_blocks:blocks()
}).

% NOTE: translated to protobuf
-record(provider_reg_distribution, {
    logical_size = 0 :: file_meta:size(),
    blocks_per_storage :: [file_distribution:storage_reg_distribution()]
}).

-record(reg_distribution, {
    distribution_per_provider = #{} :: #{
        od_provider:id() => file_distribution:provider_reg_distribution() | errors:error()
    }
}).


-record(symlink_distribution, {
    logical_size = 0 :: 0, % symlink has always 0 logical size
    storages_per_provider = #{} :: #{oneprovider:id() => [storage:id()]}
}).

-record(file_distribution_gather_result, {
    distribution ::
        file_distribution:dir_distribution() |
        file_distribution:symlink_distribution() |
        file_distribution:reg_distribution()
}).

-endif.
