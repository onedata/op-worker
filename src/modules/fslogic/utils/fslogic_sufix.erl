%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_sufix).
-author("Michal Wrzeszcz").

%% API
-export([get_new_file_location_doc/1]).

-define(IMPORTED_CONFLIOCTING_FILE_SUFFIX, <<"####IMPORTED###">>).

%%%===================================================================
%%% API
%%%===================================================================

get_new_file_location_doc(Name) ->
    <<(Name)/binary, (?IMPORTED_CONFLIOCTING_FILE_SUFFIX)/binary,
        (oneprovider:get_id())/binary>>.
