%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for symlink values recreation during bulk_download.
%%% @end
%%%--------------------------------------------------------------------
-module(bulk_download_symlinks_test).
-author("Michal Stanisz").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/errors.hrl").

-define(SPACE_PREFIX, "space_prefix").
-define(SPACE_NAME, "space_name").

-define(ROOT_PATH(Path), <<?SPACE_NAME, Path/binary>>).
-define(ABS_SYMLINK_VALUE(Path), <<?SPACE_PREFIX, Path/binary>>).

build_internal_symlinks_value_test() ->
    ?assertEqual(<<"r/t">>, bulk_download_main_process:build_internal_symlink_value(
        ?ROOT_PATH(<<"/q/w/e">>), ?ABS_SYMLINK_VALUE(<<"q/w/e/r/t">>), 2)),
    ?assertEqual(<<"../../r/t">>, bulk_download_main_process:build_internal_symlink_value(
        ?ROOT_PATH(<<"/q/w/e">>), ?ABS_SYMLINK_VALUE(<<"q/w/e/r/t">>), 4)),
    ?assertEqual(?ABS_SYMLINK_VALUE(<<"q/w/e/r/t">>), bulk_download_main_process:build_internal_symlink_value(
        ?ROOT_PATH(<<"/q/w/e/a">>), ?ABS_SYMLINK_VALUE(<<"q/w/e/r/t">>), 4)),
    
    %% @TODO VFS-8938 - properly handle symlink relative value
    ?assertEqual(<<"../../a/b/c/d/">>, bulk_download_main_process:build_internal_symlink_value(
        ?ROOT_PATH(<<"/q/w/e/a">>), <<"../../a/b/c/d/">>, 4)).

-endif.