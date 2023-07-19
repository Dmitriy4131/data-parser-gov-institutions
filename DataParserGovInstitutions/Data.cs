using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataParserGovInstitutions
{
    public class Data
    {
        public Content[] content { get; set; }
        public int totalElements { get; set; }
        public int totalPages { get; set; }
        public int totalElementsInteger { get; set; }
        public bool last { get; set; }
        public bool first { get; set; }
        public int numberOfElements { get; set; }
        public int size { get; set; }
        public int number { get; set; }
        public string sort { get; set; }

    }

    public class Content
    {
        public Common common { get; set; }
        public Indicatorsoffinancialcondition indicatorsOfFinancialCondition { get; set; }
        public Planpaymentindex[] planPaymentIndexes { get; set; }
        public object[] planPaymentIndex2020 { get; set; }
        public Planpaymentindexesmain[] planPaymentIndexesMain { get; set; }
        public Expensepaymentindex[] expensePaymentIndexes { get; set; }
        public object[] planPaymentTRU { get; set; }
        public Temporaryresourceslist[] temporaryResourcesList { get; set; }
        public Referencelist[] referenceList { get; set; }
        public Attachment[] attachments { get; set; }
    }

    public class Common
    {
        public string financialYear { get; set; }
        public string firstYearPeriod { get; set; }
        public string secondYearPeriod { get; set; }
        public object dateApprovel { get; set; }
        public object date { get; set; }
        public string lastUpdate { get; set; }
        public object founder { get; set; }
        public object founderAgencyId { get; set; }
        public object founderGlavaCode { get; set; }
        public string fullClientName { get; set; }
        public string clientRegionName { get; set; }
        public string clientRegionCode { get; set; }
        public bool isArchive { get; set; }
        public string summaryRegistryCode { get; set; }
        public string inn { get; set; }
        public string kpp { get; set; }
        public string okeiSymbol { get; set; }
        public string okeiCode { get; set; }
        public string orgType { get; set; }
    }

    public class Indicatorsoffinancialcondition
    {
        public float? sumRealEstate { get; set; }
        public float? sumRealEstateResidual { get; set; }
        public float? sumValuableProperty { get; set; }
        public float? sumValuablePropertyResidual { get; set; }
        public float? sumBalanceNoFinancial { get; set; }
        public float? cash { get; set; }
        public float? accountsCash { get; set; }
        public float? depositCash { get; set; }
        public float? others { get; set; }
        public float? sumDepthIncome { get; set; }
        public float? sumDepthExpenses { get; set; }
        public float? sumFinancialActives { get; set; }
        public float? debentures { get; set; }
        public float? kredit { get; set; }
        public float? sumDelayedPayable { get; set; }
        public float? sumObligations { get; set; }
    }

    public class Planpaymentindex
    {
        public string name { get; set; }
        public string lineCode { get; set; }
        public string kbk { get; set; }
        public float total { get; set; }
        public float? financialProvision { get; set; }
        public float? financialInsurance { get; set; }
        public float? provision781 { get; set; }
        public float? capitalInvestment { get; set; }
        public float? healthInsurance { get; set; }
        public float? serviceGrant { get; set; }
        public float? serviceTotal { get; set; }
    }

    public class Planpaymentindexesmain
    {
        public string name { get; set; }
        public string lineCode { get; set; }
        public string kbk { get; set; }
        public float total { get; set; }
        public float financialProvision { get; set; }
        public float? financialInsurance { get; set; }
        public float provision781 { get; set; }
        public float capitalInvestment { get; set; }
        public float healthInsurance { get; set; }
        public float? serviceGrant { get; set; }
        public float serviceTotal { get; set; }
    }

    public class Expensepaymentindex
    {
        public int? year { get; set; }
        public string name { get; set; }
        public string lineCode { get; set; }
        public float nextYearFz44Sum { get; set; }
        public float nextYearFz223Sum { get; set; }
        public float? nextYearTotalSum { get; set; }
        public float firstPlanYearFz44Sum { get; set; }
        public float firstPlanYearFz223Sum { get; set; }
        public float? firstPlanYearTotalSum { get; set; }
        public float secondPlanYearFz44Sum { get; set; }
        public float secondPlanYearFz223Sum { get; set; }
        public float? secondPlanYearTotalSum { get; set; }
    }


    public class Temporaryresourceslist
    {
        public string name { get; set; }
        public string lineCode { get; set; }
        public float total { get; set; }
    }

    public class Referencelist
    {
        public string name { get; set; }
        public string lineCode { get; set; }
        public float total { get; set; }
    }

    public class Attachment
    {
        public string fileName { get; set; }
        public string fileSize { get; set; }
        public string documentDate { get; set; }
        public string publishDate { get; set; }
        public string url { get; set; }
    }
}
