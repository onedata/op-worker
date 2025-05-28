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
-ifndef(DATA_DISTRIBUTION_HRL).
-define(DATA_DISTRIBUTION_HRL, 1).

%% Dir distribution

-record(provider_dir_distribution, {
    logical_size :: file_meta:size(),
    virtual_size :: file_meta:size(),
    physical_size_per_storage = #{} :: #{storage:id()  => data_distribution:dir_physical_size()},
    locations_per_storage = #{} :: data_distribution:locations_per_storage()
}).


% NOTE: translated to protobuf
-record(provider_dir_distribution_get_result, {
    current_dir_size_stats :: provider_dir_distribution:current_dir_size_stats_result(),
    locations_per_storage = #{} :: data_distribution:locations_per_storage() | errors:error()
}).

-record(dir_distribution_gather_result, {
    distribution_per_provider = #{} :: #{
        od_provider:id() => data_distribution:provider_dir_distribution() | errors:error()
    }
}).

%% Reg distribution

% NOTE: translated to protobuf
-record(provider_reg_distribution_get_result, {
    virtual_size = 0 :: file_meta:size(),
    blocks_per_storage = #{} :: #{storage:id() => fslogic_blocks:blocks()},
    locations_per_storage = #{} :: data_distribution:locations_per_storage()
}).

-record(reg_distribution_gather_result, {
    distribution_per_provider = #{} :: #{
        od_provider:id() => data_distribution:provider_reg_distribution() | errors:error()
    }
}).


%% Symlink distribution

-record(symlink_distribution_gather_result, {
    virtual_size = 0 :: 0, % symlink has always 0 virtual size
    storages_per_provider = #{} :: #{oneprovider:id() => [storage:id()]}
}).


%% Common

-record(data_distribution_gather_result, {
    distribution ::
        data_distribution:dir_distribution() |
        data_distribution:symlink_distribution() |
        data_distribution:reg_distribution()
}).

-endif.
