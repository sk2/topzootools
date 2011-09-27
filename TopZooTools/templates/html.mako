<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
	<head>                      
		<title>
			The Internet Topology Zoo
		</title>
		<link rel="stylesheet" href="blue/style.css" type="text/css" id="" media="print, projection, screen" /> 
		
		<script type="text/javascript" src="jquery-1.4.4.min.js"></script> 
		<script type="text/javascript" src="jquery.tablesorter.min.js"></script>     

		<script type="text/javascript" id="js">
		$(document).ready(function() 
		    { 
		        $("#networks").tablesorter({ sortList: [[1,0],[0,0]], widgets: ['zebra']}); 
		    } 
		);                             



		</script>      
		
		<script type="text/javascript">

		  var _gaq = _gaq || [];
		  _gaq.push(['_setAccount', 'UA-351760-4']);
		  _gaq.push(['_trackPageview']);

		  (function() {
		    var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
		    ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
		    var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
		  })();

		</script>
		
	</head>
	<body>
		<div id="wrap">
			<div id="header">
				<table class="header">
					<tr>
						<td>
							<img src="UoA_col_horz.png" />
						</td>
						<td>
							<h1>
								The Internet Topology Zoo
							</h1>
						</td>
					</tr>
				</table>
			</div>
			<div id="main">
				<div id="sidebar">
					<ul>
						<li>
							<a href="index.html">Home</a>
						</li>
						<li>
							<a href="dataset.html">Dataset</a>
						</li>               
						<li>
							<a href="gallery.html">Gallery</a>
						</li>
						<li>
							<a href="publications.html">Publications</a>
						</li>
						<li>
							<a href="toolset.html">Toolset</a>
						</li>
						<li>
							<a href="documentation.html">Documentation</a>
						</li>
						<li>
							<a href="contribute.html">Contribute</a>
						</li>
						<li>
							<a href="links.html">External Links</a>
						</li>
						<li>
							<a href="contact.html">Contact</a>
						</li>
					</ul>
				</div>
				<div id="main-content">            
					<h2> Dataset</h2>    
					<p>
					Archived datasets used in publications can be found <a href="archived_datasets.html">here</a>   
					</p>              
					<p>
					The graph and emulations for the European Interconnect model can be found <a href="eu_nren.html">here</a>   
					</p>
%if archive_file:    
	<a href="files/${archive_file}">Download</a> current dataset as a zip archive.    
	<p><br>
%endif
	                                         
	<table id="networks" class="tablesorter">     
		<thead>           
			<tr>    
				<th>Network (click for map)</th>    
				<th>Type</th>
				<th>Geo Extent</th>
				<th>Geo Location</th>         
				<th>Classification</th>
				<th>Layer</th>   
				<th>Network Date</th>  
				<th>Download</th>
				<th>Provenance</th>     
				<th>Comments</th>
			</tr>       
		</thead>                    
		<tbody>   
		%for name, data in sorted(summary_data.items()):    
			<tr>                 
				%if name in gallery_data:  
				<td><a href="maps/${name}.jpg">${data['Network']}</td>       
				%else:    
				<td>${data['Network']}</td>       
				%endif  
				<td>${data['Type']}</td>       
				<td>${data['GeoExtent']}</td>  
				<td>${data['GeoLocation']}</td>   
				<td>${data['Classification']}</td>
				<td>${data['Layer']}</td>
				<td>${data['NetworkDate']}</td>     
				<td><a href="files/${name}.gml">GML</a> <a href="files/${name}.graphml">GraphML</a></td>
				<td><a href="${data['Source']}">${data['Provenance']}</a></td>       
				<td>${data['Note']}</td>
			</tr>    
		%endfor        
		</tbody>           
	</table>              

                
	<% net_count = len(summary_data)%>
	${net_count} Networks
	<br>
	Updated ${date}           
	
	
	 	Sorting by <a href="http://tablesorter.com/">tablesorter</a> 
	   
		   			</div>
	</div>
</div>
<div id="footer">
	This project was supported by the Australian Government through an Australian Postgraduate Award and Australian Research Council Discovery Grants DP110103505 and DP0985063; and by the <a href="http://www.adelaide.edu.au">University of Adelaide</a>.
	<br>
	Last updated 2011-02-19 by simon.knight at adelaide.edu.au </a>.
</div>
</body>
</html>
	
                                        
