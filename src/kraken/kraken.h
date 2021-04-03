#ifndef KRAKEN_H
#define KRAKEN_H

/**
 * @project		Kraken
 * @file		kraken.h
 * @author		Emerson Clarke
 * @copywright	(c) 2002-2021. All Rights Reserved.
 * @date		
 * @version		1.0
 * @description	

	"We will encourage you to develop the three great virtues of a programmer: laziness, impatience, and hubris."
	http://c2.com/cgi/wiki?LazinessImpatienceHubris

 */


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#include "reason/reason.h"
#include "reason/structure/map.h"
#include "reason/structure/tree.h"
#include "reason/structure/set.h"
#include "reason/structure/index.h"
#include "reason/structure/iterator.h"
#include "reason/structure/property.h"
#include "reason/generic/generic.h"

#include "reason/system/object.h"
#include "reason/system/string.h"
#include "reason/system/storage/storage.h"
#include "reason/system/storage/archive.h"
#include "reason/system/logging/logging.h"
#include "reason/system/config.h"
#include "reason/system/time.h"
#include "reason/system/security/security.h"

#include "reason/platform/thread.h"
#include "reason/platform/process.h"

#include "reason/network/url.h"
#include "reason/network/link.h"
#include "reason/network/http/http.h"

#include "reason/language/regex/regex.h"
#include "reason/language/xml/xml.h"
#include "reason/language/sql/sql.h"


#include "reason/messaging/callback.h"

using namespace Reason::Messaging;
using namespace Reason::System;
using namespace Reason::System::Storage;
using namespace Reason::System::Logging;
using namespace Reason::System::Security;
using namespace Reason::Network;
using namespace Reason::Network::Http;
using namespace Reason::Language::Regex;
using namespace Reason::Language::Xml;

using namespace Reason::Language::Sql;
using namespace Reason::Structure;
using namespace Reason::Structure::Abstract;



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


class Sampler
{
public:

 	double Samples;
	long long Count;
	double Min;
	double Max;

	Sampler():
		Samples(0),Count(0),Min(0),Max(0)
	{

	}

	void Sample(double value)
	{
		if (Count == 0)
			Min = Max = value;
		if (value < Min)
			Min = value;
		if (value > Max)
			Max = value;
		Count ++;
		Samples += value;

	}

	double Avg()
	{
		return Samples/Count;
	}

};


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


#endif


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


