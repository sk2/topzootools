<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN">
<html>
	<head>
		<meta http-equiv="Content-type" content="text/html; charset=utf-8">
		<title>
			The Internet Topology Zoo
		</title>
		<link rel="stylesheet" href="blue/style.css" type="text/css" id="" media="print, projection, screen">
		<link rel="stylesheet" href="galleriffic-1.css" type="text/css">
		<script type="text/javascript" src="jquery-1.4.4.min.js">
</script>
		<script type="text/javascript" src="jquery.galleriffic.js">
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
							<img src="UoA_col_horz.png">
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
					<h2>
						Gallery
					</h2><!-- Start Minimal Gallery Html Containers -->
					<div id="gallery" class="content">
						<div id="controls" class="controls"></div>
						<div class="slideshow-container">
							<div id="loading" class="loader"></div>
							<div id="slideshow" class="slideshow"></div>
						</div>
					</div>
					<div id="thumbs" class="navigation">
						<ul class="thumbs noscript">
								%for name in sorted(gallery_data):
							<li>
								<a class="thumb" href="maps/${name}.jpg" title="${summary_data[name]['Network']}">${summary_data[name]['Network']}</a>
							</li>
								%endfor
							</li>
						</ul>                   
						                 
					</div><!-- End Minimal Gallery Html Containers -->      
					<div style="clear: both;"></div> 
					
				</div>                          

				
			</div>
			<div id="footer">
				This project was supported by the Australian Government through an Australian Postgraduate Award and Australian Research Council Discovery Grants DP110103505 and DP0985063; and by the <a href="http://www.adelaide.edu.au">University of Adelaide</a>.<br>
				Last updated 2011-3-15 by simon.knight at adelaide.edu.au .
			</div><script type="text/javascript">

						// We only want these styles applied when javascript is enabled
						$('div.navigation').css({'float' : 'left'});
						$('div.content').css('display', 'block');

						$(document).ready(function() {              
							// Initialize Minimal Galleriffic Gallery
							$('#thumbs').galleriffic({
								imageContainerSel:      '#slideshow',
								controlsContainerSel:   '#controls',  
								autoStart: true,     
								numThumbs: 10,
							});
						});
			</script>
		</div>
	</body>
</html>
