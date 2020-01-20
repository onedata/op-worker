%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Sufixes used by fslogic to provide functionalities:
%%% - marking file as deleted when it is opened,
%%% - generating storage names for files conflicting on storage,
%%% - generating logical names for conflicting imported files
%%%   (conflict between storage and metadata),
%%% - showing conflicts of logical files created by different providers.
%%% @end
%%%-------------------------------------------------------------------
-author("Michal Wrzeszcz").

-ifndef(FSLOGIC_SUFIX_HRL).
-define(FSLOGIC_SUFIX_HRL, 1).

-define(FILE_DELETION_LINK_SUFFIX, <<"####TO_DELETE">>).
-define(FILE_DELETION_LINK_NAME(Name),
    <<(Name)/binary, (?FILE_DELETION_LINK_SUFFIX)/binary>>).
-define(FILE_DELETION_LINK(Name, Uuid),
    {?FILE_DELETION_LINK_NAME(Name), Uuid}).


-define(CONFLICTING_STORAGE_FILE_SUFFIX_SEPARATOR, <<"%%%%">>).
-define(CONFLICTING_STORAGE_FILE_NAME(Filename,Uuid), <<Filename/binary,
    (?CONFLICTING_STORAGE_FILE_SUFFIX_SEPARATOR)/binary, Uuid/binary>>).

-define(IMPORTED_CONFLICTING_FILE_SUFFIX_SEPARATOR, <<"####IMPORTED###">>).
-define(IMPORTED_CONFLICTING_FILE_NAME(Name),
    <<(Name)/binary, (?IMPORTED_CONFLICTING_FILE_SUFFIX_SEPARATOR)/binary,
        (oneprovider:get_id())/binary>>).

-define(CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR_CHAR, "@").
-define(CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR, <<?CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR_CHAR>>).

-endif.